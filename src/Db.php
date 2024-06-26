<?php

declare(strict_types=1);

namespace Tianyage\SimpleDb;

use Exception;
use PDO;
use PDOException;
use Throwable;

class Db
{
    
    //PDO实例
    private PDO $db;
    //单例模式 本类对象引用
    private static object $instance;
    
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
     * 私有克隆
     */
    private function __clone()
    {
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
        $stmt         = $this->query($sql, $values);
        return $this->db->lastInsertId();
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
     * @return mixed
     * @throws Exception
     */
    public function select(string $table, string $where, string $fields = '*', int $limit = 0, string $order = ''): mixed
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
    
    
    private function query($sql, $params = [])
    {
        $stmt = $this->db->prepare($sql);
        $stmt->execute($params);
        return $stmt;
    }
    
    public function exec($sql)
    {
        return $this->db->exec($sql);
    }
    
    /**
     * 获取配置
     *
     * @param string       $name
     * @param array|string $default
     *
     * @return array|string
     * @throws Exception
     */
    private static function getConfig(string $name = '', array|string $default = ''): array|string
    {
        static $config = null;
        if (!$config) {
            $lib_path    = realpath(dirname(__DIR__)) . DIRECTORY_SEPARATOR; // D:\WorkSpace\Git\qq-utils\vendor\tianyage\simple-db\
            $root_path   = dirname($lib_path, 3) . DIRECTORY_SEPARATOR; // D:\WorkSpace\Git\qq-utils\
            $config_path = "{$root_path}config" . DIRECTORY_SEPARATOR . "simple-db.php";
            try {
                $config = require $config_path;
            } catch (Throwable $e) {
                echo "文件打开失败：{$config_path}";
                die;
            }
        }
        // 无参数时获取所有
        if (empty($name)) {
            return $config;
        }
        
        if (!str_contains($name, '.')) {
            return $config[$name] ?? [];
        }
        
        $name    = explode('.', $name);
        $name[0] = strtolower($name[0]);
        //        $config  = self::$config;
        
        // 按.拆分成多维数组进行判断
        foreach ($name as $val) {
            if (isset($config[$val])) {
                $config = $config[$val];
            } else {
                return $default;
            }
        }
        
        return $config;
    }
    
}