<?php

declare(strict_types=1);

namespace Tianyage\SimpleDb;

use Exception;
use PDO;
use PDOException;
use PDOStatement;
use Throwable;

class Db
{
    
    //PDO实例
    private PDO $db;
    //单例模式 本类对象引用
    private static ?object $instance;
    private static ?array  $config;
    
    /**
     * 私有构造方法
     *
     * @throws Throwable
     */
    private function __construct()
    {
        // 连接服务器
        try {
            $this->connect();
        } catch (Throwable $e) {
            echo $e->getMessage();
            die;
        }
    }
    
    /**
     * 防止克隆
     */
    private function __clone()
    {
    }
    
    
    /**
     * 获取配置
     *
     * @param string       $name    配置项名
     * @param array|string $default 设置一个没找到时的默认值
     *
     * @return array|string
     * @throws Exception
     */
    private static function getConfig(string $name = '', array|string $default = ''): array|string
    {
        if (empty(self::$config)) {
            $lib_path    = realpath(dirname(__DIR__)) . DIRECTORY_SEPARATOR; // D:\WorkSpace\Git\qq-utils\vendor\tianyage\simple-db\
            $root_path   = dirname($lib_path, 3) . DIRECTORY_SEPARATOR; // D:\WorkSpace\Git\qq-utils\
            $config_path = "{$root_path}config" . DIRECTORY_SEPARATOR . "simple-db.php";
            
            //            //todo 正式环境请注释下面代码
            //            $config_path = "D:\\WorkSpace\Git\simple-db\config\simple-db.php";
            
            if (!file_exists($config_path)) {
                throw new Exception("配置文件不存在: {$config_path}");
            }
            try {
                self::$config = require $config_path;
            } catch (Throwable $e) {
                echo "{$config_path}文件打开失败：" . $e->getMessage();
                die;
            }
            if (!is_array(self::$config)) {
                throw new Exception("配置文件格式错误，期望返回数组: {$config_path}");
            }
        }
        
        // 如果未指定具体配置项，返回全部配置
        if (empty($name)) {
            return self::$config;
        }
        
        // 判断是否获取多级配置
        if (str_contains($name, '.')) {
            $name = explode('.', $name); // 用.来分割多级配置项名
            
            // 如果.后没提供字符串，那就默认提供.前的所有配置项  例$name是[app.]那就获取app下的所有配置
            if (!isset($config[$name[1]])) {
                return $config[$name[0]] ?? [];
            } else {
                return $config[$name[0]][$name[1]] ?? $default;
            }
        } else {
            return self::$config[$name] ?? $default;
        }
    }
    
    /**
     * 获得单例对象
     *
     * @return object 单例的对象
     */
    public static function getInstance(): object
    {
        if (empty(self::$instance)) {
            self::$instance = new self();
        }
        return self::$instance; //返回对象
    }
    
    /**
     * 连接目标服务器
     *
     * @return void
     * @throws Exception
     */
    private function connect(): void
    {
        $config = self::getConfig();
        try {
            $this->db = new PDO(
                "mysql:host={$config['hostname']};port={$config['hostport']};dbname={$config['database']}",
                $config['username'],
                $config['password'],
                [
                    //                    // 设置结果集返回类型为关联数组
                    //                    PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC,
                    //                    // 禁止模拟预处理语句
                    //                    PDO::ATTR_EMULATE_PREPARES   => false,
                    //                    // 关闭结果集自动转换数据类型
                    //                    PDO::ATTR_STRINGIFY_FETCHES  => false,
                    
                    // 启用持久连接 以减少数据库连接的开销
                    PDO::ATTR_PERSISTENT         => true,
                    // 当发生错误时，PDO 将抛出一个异常（PDOException）
                    PDO::ATTR_ERRMODE            => PDO::ERRMODE_EXCEPTION,
                    // 设置字符集
                    PDO::MYSQL_ATTR_INIT_COMMAND => 'SET NAMES ' . $config['charset'],
                ]
            );
        } catch (PDOException $e) {
            // 错误提示
            throw new Exception('链接数据库失败:' . $e->getMessage());
        }
    }
    
    /**
     * 断线重连机制
     *
     * @throws Exception
     */
    private function reconnect(): void
    {
        try {
            $this->connect();
        } catch (Exception $e) {
            throw new Exception('重连数据库失败: ' . $e->getMessage());
        }
    }
    
    /**
     * 查询方法，带自动重连
     *
     * @throws Exception
     */
    private function query(string $sql, array $params = []): PDOStatement
    {
        try {
            $stmt = $this->db->prepare($sql);
            $stmt->execute($params);
            return $stmt;
        } catch (PDOException $e) {
            // 判断数据连接超时
            if (str_contains($e->getMessage(), 'server has gone away')) {
                $this->reconnect();
                return $this->query($sql, $params); // 重试
            }
            throw new Exception('SQL执行失败: ' . $e->getMessage());
        }
    }
    
    
    /**
     * 增加数据
     *
     * @param string $table 表名
     * @param array  $data  k=>v数组  ['user'=>1]
     *
     * @return false|string
     * @throws Exception
     */
    public function insert(string $table, array $data): bool|string
    {
        $config       = self::getConfig();
        $columns      = array_keys($data);
        $values       = array_values($data);
        $placeholders = str_repeat('?,', count($values) - 1) . '?';
        $sql          = "INSERT INTO {$config['prefix']}{$table} (" . implode(',', $columns) . ") VALUES ({$placeholders})";
        $this->query($sql, $values);
        return $this->db->lastInsertId();
    }
    
    /**
     * 批量插入数据
     *
     * @param string $table
     * @param array  $data
     * @param int    $ignoreDuplicate 是否忽略主键冲突，默认0  1：INSERT IGNORE INTO  2：ON DUPLICATE KEY UPDATE
     *
     * @return int 影响的行数（即插入的记录数，失败返回0）
     * @throws Exception
     */
    public function insertBatch(string $table, array $data, int $ignoreDuplicate = 0): int
    {
        $config = self::getConfig();
        
        // 获取表字段名
        $columns      = array_keys($data[0]);
        $placeholders = str_repeat('?,', count($columns) - 1) . '?';
        
        // 构建批量插入的 SQL 语句
        $sql = "INSERT INTO {$config['prefix']}{$table} (" . implode(',', $columns) . ") VALUES ";
        
        // 准备值的数组
        $values = [];
        foreach ($data as $row) {
            $values = array_merge($values, array_values($row));
        }
        
        // 为每一行生成插入的占位符
        $sql .= str_repeat("($placeholders),", count($data) - 1) . "($placeholders)";
        
        // 根据是否忽略主键冲突，选择不同的插入策略
        if ($ignoreDuplicate === 1) {
            // 使用 INSERT IGNORE 语句忽略主键冲突
            $sql = str_replace("INSERT INTO", "INSERT IGNORE INTO", $sql);
        } elseif ($ignoreDuplicate === 2) {
            // 使用 ON DUPLICATE KEY UPDATE，更新冲突的记录
            $updateClause = [];
            foreach ($columns as $column) {
                $updateClause[] = "$column = VALUES($column)";
            }
            $sql .= " ON DUPLICATE KEY UPDATE " . implode(', ', $updateClause);
        }
        
        // 执行查询
        $stmt = $this->query($sql, $values);
        
        // 返回插入的条数
        return $stmt->rowCount(); // 返回影响的行数，即插入的记录数
    }
    
    
    /**
     * 删除
     *
     * @param string $table 表名
     * @param string $where a=1 and b=2
     *
     * @return int 影响行数
     * @throws Exception
     */
    public function delete(string $table, string $where): int
    {
        $config = self::getConfig();
        $sql    = "DELETE FROM {$config['prefix']}{$table} WHERE {$where}";
        $stmt   = $this->query($sql);
        return $stmt->rowCount();
    }
    
    /**
     * 执行查询语句，返回所有结果
     *
     * @param string $table
     * @param string $where
     * @param string $fields 需要返回的字段
     * @param int    $limit
     * @param string $order
     *
     * @return array|bool
     * @throws Exception
     */
    public function select(string $table, string $where, string $fields = '*', int $limit = 0, string $order = ''): array|bool
    {
        $config = self::getConfig();
        //        if (count($fields) > 1) {
        //            $columns = implode(',', array_values($fields));
        //        } else {
        //            $columns = $fields[0];
        //        }
        $sql = "SELECT {$fields} FROM {$config['prefix']}{$table} WHERE {$where}";
        if ($order) {
            $sql .= " order by {$order}";
        }
        if ($limit > 0) {
            $sql .= " limit {$limit}";
        }
        $stmt = $this->db->query($sql);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }
    
    /**
     * 执行查询语句 获取首条结果
     *
     * @param string $table
     * @param string $where
     * @param string $fields
     *
     * @return mixed
     * @throws Exception
     */
    public function find(string $table, string $where, string $fields = '*'): mixed
    {
        $config = self::getConfig();
        //        if (count($fields) > 1) {
        //            $columns = implode(',', array_values($fields));
        //        } else {
        //            $columns = $fields[0];
        //        }
        $sql  = "SELECT {$fields} FROM {$config['prefix']}{$table} WHERE {$where} limit 1";
        $stmt = $this->db->query($sql);
        return $stmt->fetch(PDO::FETCH_ASSOC);
    }
    
    /**
     * 更新数据
     *
     * @param string $table
     * @param array  $data
     * @param string $where
     *
     * @return int 影响行输
     * @throws Exception
     */
    public function update(string $table, array $data, string $where): int
    {
        $config    = self::getConfig();
        $setClause = '';
        $params    = [];
        foreach ($data as $key => $value) {
            $setClause .= "{$key}=?,";
            $params[]  = $value;
        }
        $setClause = rtrim($setClause, ',');
        $sql       = "UPDATE {$config['prefix']}{$table} SET {$setClause} WHERE {$where}";
        $stmt      = $this->query($sql, $params);
        return $stmt->rowCount();
    }
    
    public function exec($sql): bool|int
    {
        return $this->db->exec($sql);
    }
    
    /**
     * 事务支持
     */
    public function beginTransaction(): void
    {
        $this->db->beginTransaction();
    }
    
    public function commit(): void
    {
        $this->db->commit();
    }
    
    public function rollBack(): void
    {
        $this->db->rollBack();
    }
}