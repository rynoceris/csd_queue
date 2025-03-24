#!/usr/local/bin/php
<?php
declare(strict_types=1);

$logFile = '/home/collegesportsdir/logs/csd_api/csdapi.log';
$timestamp = date('Y-m-d H:i:s');
error_log("[$timestamp] Process Queue Script Started - SAPI: " . php_sapi_name() . "\n", 3, $logFile);

// Force CLI mode
if (php_sapi_name() !== 'cli') {
	error_log("[$timestamp] Error: Not running in CLI mode (SAPI: " . php_sapi_name() . ")\n", 3, $logFile);
	die("This script can only be run from the command line. SAPI detected: " . php_sapi_name() . "\n");
}

require_once __DIR__ . '/csdapi_queue.php';

// Set script execution time limit
set_time_limit(7200);

$logger = Logger::getInstance();
$handler = new WebhookHandler();

// Add heartbeat log
$heartbeatMsg = "=== Queue Processor Heartbeat: " . date('Y-m-d H:i:s') . " ===";
$logger->log($heartbeatMsg);

$startTime = time();
$maxRuntime = 7200; // 2 hour runtime

try {
	$logger->log("=== Queue Processor Started ===");
	
	while (true) {
		// Check runtime
		if (time() - $startTime >= $maxRuntime) {
			$logger->log("Maximum runtime reached, shutting down...");
			break;
		}

		// Process queue item
		$result = $handler->processQueueItem();
		
		// If no items were processed, break
		if ($result === false) {
			$logger->log("No items in queue to process");
			break;
		}
		
		// Memory management
		if (gc_enabled()) {
			gc_collect_cycles();
		}
		
		// Sleep for a short period to prevent CPU overuse
		usleep(100000); // 100ms
	}
} catch (Exception $e) {
	$logger->log("Queue processing error: " . $e->getMessage(), 'ERROR');
} finally {
	$logger->log("=== Queue Processor Completed ===");
}
?>
