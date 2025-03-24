<?php
// Prevent direct browser access
if (php_sapi_name() !== 'cli') {
	header('HTTP/1.0 403 Forbidden');
	exit('This script can only be run from the command line');
}

echo "Script starting...\n";

// Configuration
$config = [
	'logFile' => '/home/collegesportsdir/logs/csd_api/csdapi.log',
	'archiveDir' => '/home/collegesportsdir/logs/csd_api/archive',
	'emailTo' => 'ryan@texontowel.com',
	'emailFrom' => 'server@collegesportsdirectory.com',
	'serverName' => 'College Sports Directory'
];

function sendErrorEmail($config, $error) {
	$subject = "[{$config['serverName']}] Log Rotation Error";
	$message = "An error occurred during log rotation:\n\n";
	$message .= $error . "\n\n";
	$message .= "Time: " . date('Y-m-d H:i:s') . "\n";
	$message .= "Server: " . php_uname('n') . "\n";
	
	$headers = [
		'From: ' . $config['emailFrom'],
		'X-Mailer: PHP/' . phpversion(),
		'Content-Type: text/plain; charset=UTF-8'
	];
	
	return mail(
		$config['emailTo'],
		$subject,
		$message,
		implode("\r\n", $headers)
	);
}

try {
	echo "Checking log file...\n";
	if (!file_exists($config['logFile'])) {
		throw new RuntimeException("Log file does not exist");
	}

	$logSize = filesize($config['logFile']);
	echo "Log file exists with size: $logSize bytes\n";

	echo "Checking archive directory...\n";
	if (!file_exists($config['archiveDir'])) {
		echo "Archive directory does not exist, creating it...\n";
		if (!mkdir($config['archiveDir'], 0755, true)) {
			throw new RuntimeException("Failed to create archive directory");
		}
		echo "Successfully created archive directory\n";
	}

	echo "Starting log rotation...\n";

	// Create archive filename with timestamp
	$timestamp = date('Y-m-d');
	$archiveFile = $config['archiveDir'] . '/csdapi_' . $timestamp . '.log.gz';
	echo "Archive file will be: $archiveFile\n";

	// Copy current log to archive
	if (file_exists($config['logFile']) && filesize($config['logFile']) > 0) {
		echo "Opening files for compression...\n";
		
		// Open the files
		$fp = fopen($config['logFile'], 'r');
		if (!$fp) {
			throw new RuntimeException("Could not open log file for reading");
		}
		
		$zp = gzopen($archiveFile, 'w9'); // 9 is maximum compression
		if (!$zp) {
			fclose($fp);
			throw new RuntimeException("Could not open archive file for writing");
		}
		
		echo "Copying data to compressed archive...\n";
		// Copy data
		$totalBytes = 0;
		while (!feof($fp)) {
			$data = fread($fp, 8192);
			if ($data === false) {
				throw new RuntimeException("Error reading from log file");
			}
			$bytes = gzwrite($zp, $data);
			if ($bytes === false) {
				throw new RuntimeException("Error writing to archive file");
			}
			$totalBytes += strlen($data);
		}
		
		// Close the files
		fclose($fp);
		gzclose($zp);
		
		echo "Compressed $totalBytes bytes successfully\n";
		
		// Verify archive was created
		if (file_exists($archiveFile)) {
			$archiveSize = filesize($archiveFile);
			echo "Archive created successfully: $archiveSize bytes\n";
			
			// Clear the current log file
			echo "Clearing current log file...\n";
			if (file_put_contents($config['logFile'], '') === false) {
				throw new RuntimeException("Failed to clear log file");
			}
			echo "Log file cleared successfully\n";
		} else {
			throw new RuntimeException("Archive file was not created");
		}
	}

	echo "Checking for old archives to remove...\n";
	$files = glob($config['archiveDir'] . '/csdapi_*.log.gz');
	foreach ($files as $file) {
		$fileDate = strtotime(substr(basename($file, '.log.gz'), 7)); // Skip "csdapi_" prefix
		if ($fileDate && $fileDate < strtotime("-7 days")) {
			echo "Removing old archive: " . basename($file) . "\n";
			if (!unlink($file)) {
				throw new RuntimeException("Failed to remove old archive: $file");
			}
			echo "Successfully removed old archive\n";
		}
	}

	echo "Script completed successfully\n";

} catch (Exception $e) {
	$errorMessage = "Log rotation failed: " . $e->getMessage();
	echo $errorMessage . "\n";
	
	if (sendErrorEmail($config, $errorMessage)) {
		echo "Error notification email sent\n";
	} else {
		echo "Failed to send error notification email\n";
	}
	exit(1);
}
?>
