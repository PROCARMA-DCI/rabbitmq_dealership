<?php
// File: /var/www/html/rabbitmq-proxy.php
// Fixed version with correct autoload path

ini_set('display_errors', 1);
ini_set('display_startup_errors', 1);
error_reporting(E_ALL);

// Try multiple autoload paths
$autoload_paths = [
    '/var/www/html/vendor/autoload.php',          // Moved to Apache directory
    '/home/ubuntu/php-rabbitmq-test/vendor/autoload.php',  // Original location
    '/opt/dealership-consumer/vendor/autoload.php'         // Alternative location
];

$autoload_found = false;
foreach ($autoload_paths as $path) {
    if (file_exists($path) && is_readable($path)) {
        require_once $path;
        $autoload_found = $path;
        break;
    }
}

if (!$autoload_found) {
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => 'Composer autoload not found or not readable',
        'checked_paths' => $autoload_paths,
        'current_user' => get_current_user(),
        'php_version' => PHP_VERSION
    ]);
    exit;
}

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Message\AMQPMessage;

header('Content-Type: application/json');
header('Access-Control-Allow-Origin: *');
header('Access-Control-Allow-Methods: POST, GET');

$log_file = './rabbitmq-proxy.log';

function debug_log($message) {
    global $log_file;
    file_put_contents($log_file, date('Y-m-d H:i:s') . " - " . $message . "\n", FILE_APPEND);
}

try {
    debug_log("Proxy request from: " . ($_SERVER['REMOTE_ADDR'] ?? 'unknown'));
    debug_log("Using autoload: $autoload_found");
    
    $method = $_SERVER['REQUEST_METHOD'];
    
    // Allow GET for testing
    if ($method === 'GET') {
        echo json_encode([
            'success' => true,
            'message' => 'RabbitMQ HTTP Proxy is running',
            'php_version' => PHP_VERSION,
            'autoload_path' => $autoload_found,
            'timestamp' => date('Y-m-d H:i:s'),
            'amqp_available' => class_exists('PhpAmqpLib\Connection\AMQPStreamConnection')
        ]);
        exit;
    }
    
    if ($method !== 'POST') {
        throw new Exception('Only GET and POST requests allowed');
    }
    
    // Security
    $secret_key = 'dealership_rabbitmq_proxy_2025';
    $provided_key = $_POST['secret_key'] ?? '';
    
    if ($provided_key !== $secret_key) {
        throw new Exception('Invalid secret key');
    }
    
    $action = $_POST['action'] ?? '';
    debug_log("Action: $action");
    
    // RabbitMQ configuration
    $rabbitmq_config = [
        'host' => 'localhost',
        'port' => 5672,
        'user' => 'dealership_user',
        'password' => 'secure_password_123',
        'vhost' => 'dealership_vhost'
    ];
    
    if ($action === 'test_connection') {
        debug_log("Testing RabbitMQ connection...");
        
        // Test RabbitMQ connection
        $connection = new AMQPStreamConnection(
            $rabbitmq_config['host'],
            $rabbitmq_config['port'],
            $rabbitmq_config['user'],
            $rabbitmq_config['password'],
            $rabbitmq_config['vhost']
        );
        
        $channel = $connection->channel();
        $channel->close();
        $connection->close();
        
        debug_log("RabbitMQ connection successful");
        
        echo json_encode([
            'success' => true,
            'message' => 'RabbitMQ connection test successful via HTTP proxy',
            'autoload_path' => $autoload_found,
            'timestamp' => date('Y-m-d H:i:s')
        ]);
        
    } elseif ($action === 'publish_message') {
        debug_log("Publishing message to RabbitMQ...");
        
        $exchange = $_POST['exchange'] ?? '';
        $routing_key = $_POST['routing_key'] ?? '';
        $message_body = $_POST['message'] ?? '';
        
        if (empty($exchange) || empty($routing_key) || empty($message_body)) {
            throw new Exception('Missing required parameters: exchange, routing_key, message');
        }
        
        debug_log("Exchange: $exchange, Message body: $message_body, Routing Key: $routing_key");
        
        // Connect to RabbitMQ
        $connection = new AMQPStreamConnection(
            $rabbitmq_config['host'],
            $rabbitmq_config['port'],
            $rabbitmq_config['user'],
            $rabbitmq_config['password'],
            $rabbitmq_config['vhost']
        );
        
        $channel = $connection->channel();
        
        // Declare exchange and queue
        $channel->exchange_declare($exchange, 'direct', false, true, false);
        $channel->queue_declare('service_redemption_queue', false, true, false, false);
        $channel->queue_bind('service_redemption_queue', $exchange, $routing_key);
        
        // Create and publish message
        $message = new AMQPMessage($message_body, [
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'content_type' => 'application/json',
            'timestamp' => time()
        ]);
        
        $channel->basic_publish($message, $exchange, $routing_key);
        
        // Get queue status
        $queue_info = $channel->queue_declare('service_redemption_queue', true);
        $message_count = $queue_info[1];
        
        $channel->close();
        $connection->close();
        
        // Parse message to get ID
        $message_data = json_decode($message_body, true);
        $message_id = $message_data['message_id'] ?? 'unknown';
        
        debug_log("Message published successfully. ID: $message_id, Queue count: $message_count");
        
        echo json_encode([
            'success' => true,
            'message' => 'Message published successfully via HTTP proxy',
            'message_id' => $message_id,
            'queue_count' => $message_count,
            'timestamp' => date('Y-m-d H:i:s')
        ]);
        
    } else {
        throw new Exception('Invalid action. Supported: test_connection, publish_message');
    }
    
} catch (Exception $e) {
    debug_log("ERROR: " . $e->getMessage());
    
    http_response_code(500);
    echo json_encode([
        'success' => false,
        'error' => $e->getMessage(),
        'autoload_path' => $autoload_found ?? 'not found',
        'timestamp' => date('Y-m-d H:i:s')
    ]);
}

debug_log("Request completed\n");
?>
