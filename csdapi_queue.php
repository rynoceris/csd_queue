<?php

// At the start of your script
define('MAX_RUNTIME', 7200); // 120 minutes
$startTime = time();

if (!function_exists('mysqli_ping')) {
	$this->logger->log("CRITICAL: mysqli_ping function doesn't exist!", 'ERROR');
}

// Full error reporting
error_reporting(E_ALL);
ini_set('display_errors', 1);
set_error_handler(function($errno, $errstr, $errfile, $errline) {
	$logger = Logger::getInstance();
	$logger->log("PHP ERROR: [$errno] $errstr in $errfile:$errline", 'ERROR');
	return true;
});

// Load environment variables from .env file SILENTLY (no logging yet)
$envPath = '/home/collegesportsdir/config/.env';
$dbHost = '';
$dbUser = '';
$dbName = '';

if (file_exists($envPath)) {
	// Read the file line by line instead of using parse_ini_file
	$lines = file($envPath, FILE_IGNORE_NEW_LINES | FILE_SKIP_EMPTY_LINES);
	foreach ($lines as $line) {
		// Skip comments
		if (strpos(trim($line), '#') === 0) {
			continue;
		}
		
		// Split by the first equals sign
		$parts = explode('=', $line, 2);
		if (count($parts) >= 2) {
			$key = trim($parts[0]);
			$value = trim($parts[1]);
			
			// Remove surrounding quotes if present
			if ((substr($value, 0, 1) === '"' && substr($value, -1) === '"') || 
				(substr($value, 0, 1) === "'" && substr($value, -1) === "'")) {
				$value = substr($value, 1, -1);
			}
			
			// Set environment variable
			putenv("$key=$value");
			
			// Remember DB settings for later logging (but don't log yet!)
			if ($key === 'DB_HOST') $dbHost = $value;
			if ($key === 'DB_USER') $dbUser = $value;
			if ($key === 'DB_NAME') $dbName = $value;
		}
	}
}

// Configuration constants loaded from environment
define('LOG_FILE', getenv('LOG_FILE') ?: '/home/collegesportsdir/logs/csd_api/csdapi.log');
define('OPENAI_API_KEY', getenv('OPENAI_API_KEY'));
define('DB_HOST', getenv('DB_HOST') ?: 'localhost');
define('DB_USER', getenv('DB_USER') ?: 'collegesportsdir');
define('DB_PASSWORD', getenv('DB_PASSWORD') ?: '');
define('DB_NAME', getenv('DB_NAME') ?: 'collegesportsdir');
define('PDB_USERNAME', getenv('PDB_USERNAME'));
define('PDB_PASSWORD', getenv('PDB_PASSWORD'));
define('PDB_API_BASE', getenv('PDB_API_BASE'));
define('LOCK_DIR', getenv('LOCK_DIR') ?: '/home/collegesportsdir/public_html/locks');
define('QUEUE_DIR', getenv('QUEUE_DIR') ?: '/home/collegesportsdir/public_html/queue');
define('API_KEY', getenv('API_KEY'));
define('USE_TEST_SCHOOL', getenv('USE_TEST_SCHOOL') === 'true');
define('CACHE_DURATION', getenv('CACHE_DURATION') ?: 3600);
define('MAX_QUEUE_AGE', getenv('MAX_QUEUE_AGE') ?: 86400);
define('RATE_LIMIT_MAX', getenv('RATE_LIMIT_MAX') ?: 100);
define('RATE_LIMIT_WINDOW', getenv('RATE_LIMIT_WINDOW') ?: 3600);
define('QUEUE_PROCESSOR_MAX_RUNTIME', getenv('QUEUE_PROCESSOR_MAX_RUNTIME') ?: 7200);
define('QUEUE_PROCESSOR_SLEEP_TIME', getenv('QUEUE_PROCESSOR_SLEEP_TIME') ?: 100000);
define('QUEUE_PROCESSOR_ERROR_SLEEP_TIME', getenv('QUEUE_PROCESSOR_ERROR_SLEEP_TIME') ?: 5);
define('MAX_PARALLEL_PROCESSES', 4); // Adjust based on your server resources
define('MYSQL_WAIT_TIMEOUT', getenv('MYSQL_WAIT_TIMEOUT') ?: 600);
define('MYSQL_INTERACTIVE_TIMEOUT', getenv('MYSQL_INTERACTIVE_TIMEOUT') ?: 600);
define('MYSQL_MAX_ALLOWED_PACKET', getenv('MYSQL_MAX_ALLOWED_PACKET') ?: '16M');
define('MYSQL_NET_READ_TIMEOUT', getenv('MYSQL_NET_READ_TIMEOUT') ?: 120);
define('MYSQL_NET_WRITE_TIMEOUT', getenv('MYSQL_NET_WRITE_TIMEOUT') ?: 120);

// Make sure directories exist
foreach ([dirname(LOG_FILE), LOCK_DIR, QUEUE_DIR] as $dir) {
	if (!file_exists($dir)) {
		mkdir($dir, 0755, true);
	}
}

trait StringSanitizerTrait {
	protected function sanitizeString($string) {
		if (!is_string($string)) {
			if (isset($this->logger)) {
				$this->logger->log("Non-string value passed to sanitizeString: " . gettype($string), 'WARNING');
			}
			return '';
		}
		
		// Log problematic strings before sanitization
		if (isset($this->logger)) {
			if (strpos($string, "\t") !== false) {
				$this->logger->log("Found tab character in string before sanitization: " . bin2hex($string), 'WARNING');
			}
			if (preg_match('/\s{2,}/', $string)) {
				$this->logger->log("Found multiple spaces in string before sanitization: " . bin2hex($string), 'WARNING');
			}
			if (strpos($string, "\xC2\xA0") !== false) {
				$this->logger->log("Found non-breaking space in string before sanitization: " . bin2hex($string), 'WARNING');
			}
		}
		
		// Convert various types of spaces/whitespace to regular space
		$string = str_replace([
			"\t",          // Tab
			"\n",          // Newline
			"\r",          // Carriage return
			"\0",          // Null byte
			"\x0B",        // Vertical tab
			"\xC2\xA0"    // Non-breaking space
		], ' ', $string);
		
		// Remove multiple spaces
		$string = preg_replace('/\s+/', ' ', $string);
		
		// Trim whitespace from beginning and end
		$result = trim($string);
		
		// Log final sanitized string if it changed significantly
		if (isset($this->logger) && $result !== $string) {
			$this->logger->log("String sanitized from: " . bin2hex($string) . " to: " . bin2hex($result), 'DEBUG');
		}
		
		return $result;
	}
}

class ConfigValidator {
	public static function validate() {
		$required = [
			'OPENAI_API_KEY', 'PDB_USERNAME', 'PDB_PASSWORD',
			'LOG_FILE', 'LOCK_DIR', 'QUEUE_DIR', 'API_KEY'
		];
		
		foreach ($required as $key) {
			if (!defined($key) || empty(constant($key))) {
				throw new RuntimeException("Missing required configuration: $key");
			}
		}
		
		// Validate directories exist and are writable
		foreach ([LOG_FILE, LOCK_DIR, QUEUE_DIR] as $path) {
			if (!file_exists($path)) {
				if (!mkdir(dirname($path), 0755, true)) {
					throw new RuntimeException("Failed to create directory: " . dirname($path));
				}
			}
			if (!is_writable($path)) {
				throw new RuntimeException("Path not writable: $path");
			}
		}
	}
}

class Logger {
	private static $instance = null;
	private $lockFile;
	private $requestId;
	
	public static function getInstance() {
		if (self::$instance === null) {
			self::$instance = new self();
		}
		return self::$instance;
	}

	public function __construct() {
		$this->requestId = uniqid('req_', true);
		$this->lockFile = LOCK_DIR . '/csdapi.lock';
	}

	public function log($message, $level = 'INFO') {
		try {
			if (!is_writable(LOG_FILE)) {
				error_log("Log file not writable: " . LOG_FILE);
				return;
			}

			$fp = fopen($this->lockFile, 'w');
			if (flock($fp, LOCK_EX)) {  // Exclusive lock
				$timestamp = (new DateTime('now', new DateTimeZone('America/New_York')))
					->format('d-M-Y H:i:s T');
				$memoryUsage = $this->formatBytes(memory_get_usage(true));
				$logMessage = "[$this->requestId][$timestamp][$level][$memoryUsage] $message\n";
				
				$result = error_log($logMessage, 3, LOG_FILE);
				
				if ($result === false) {
					error_log("Failed to write to custom log file: " . LOG_FILE);
					error_log("Attempted to log: $message");
				}
				
				flock($fp, LOCK_UN);  // Release lock
			}
			fclose($fp);
		} catch (Exception $e) {
			error_log("Logger error: " . $e->getMessage());
		}
	}

	public function logArray($array) {
		$this->log(print_r($array, true));
	}

	private function formatBytes($bytes) {
		$units = ['B', 'KB', 'MB', 'GB'];
		$bytes = max($bytes, 0);
		$pow = floor(($bytes ? log($bytes) : 0) / log(1024));
		$pow = min($pow, count($units) - 1);
		$bytes /= pow(1024, $pow);
		return round($bytes, 2) . ' ' . $units[$pow];
	}
}

// NOW we can create the logger instance
$logger = Logger::getInstance();
$logger->log("=== Script Started ===");
$logger->log("PHP SAPI: " . php_sapi_name());
$logger->log("Running from CLI: " . (php_sapi_name() === 'cli' ? 'Yes' : 'No'));

// Log the environment variables here - AFTER logger is initialized
$logger->log("Environment variables loaded from: $envPath");
$logger->log("Database configuration - Host: $dbHost, User: $dbUser, Database: $dbName");

// Test database connection
$logger->log("Testing direct database connection...");
try {
	$testConn = new mysqli(DB_HOST, DB_USER, DB_PASSWORD, DB_NAME);
	
	if ($testConn->connect_error) {
		throw new RuntimeException("Database connection failed: " . $testConn->connect_error);
	}
	
	$logger->log("Direct database connection successful!");
	$testConn->close();
	
	// Continue with other class definitions and tests
} catch (Exception $e) {
	$logger->log("Database connection failed: " . $e->getMessage(), 'ERROR');
	die("Database connection failed: " . $e->getMessage());
}

class Cache {
	private $cacheDir;
	private $logger;

	public function __construct() {
		$this->cacheDir = sys_get_temp_dir() . '/webhook_cache';
		if (!file_exists($this->cacheDir)) {
			mkdir($this->cacheDir, 0755, true);
		}
		$this->logger = Logger::getInstance();
	}

	public function get($key) {
		$cacheFile = $this->getCacheFilePath($key);
		if (file_exists($cacheFile) && (time() - filemtime($cacheFile)) < CACHE_DURATION) {
			$this->logger->log("Cache hit for key: $key", 'DEBUG');
			return unserialize(file_get_contents($cacheFile));
		}
		$this->logger->log("Cache miss for key: $key", 'DEBUG');
		return null;
	}

	public function set($key, $value) {
		$cacheFile = $this->getCacheFilePath($key);
		file_put_contents($cacheFile, serialize($value));
		$this->logger->log("Cached value for key: $key", 'DEBUG');
	}

	private function getCacheFilePath($key) {
		return $this->cacheDir . '/' . md5($key);
	}

	public function cleanup() {
		foreach (glob($this->cacheDir . '/*') as $file) {
			if (time() - filemtime($file) > CACHE_DURATION) {
				unlink($file);
			}
		}
	}
}

class RateLimiter {
	private $cache;
	private $logger;

	public function __construct() {
		$this->cache = new Cache();
		$this->logger = Logger::getInstance();
	}

	public function checkLimit($ip) {
		$key = "rate_limit:$ip";
		$requests = $this->cache->get($key) ?: [];
		
		// Remove old requests
		$requests = array_filter($requests, function($timestamp) {
			return $timestamp > time() - RATE_LIMIT_WINDOW;
		});
		
		if (count($requests) >= RATE_LIMIT_MAX) {
			$this->logger->log("Rate limit exceeded for IP: $ip", 'WARNING');
			return false;
		}
		
		$requests[] = time();
		$this->cache->set($key, $requests);
		return true;
	}
}

class OpenAiClient {
	private $apiKey;
	private $apiUrl = 'https://api.openai.com/v1/chat/completions';
	private $logger;
	private $cache;
	private $connectionTimeout = 20;  // Connection timeout in seconds
	private $totalTimeout = 125;      // Total timeout in seconds
	
	public function __construct($apiKey) {
		$this->apiKey = $apiKey;
		$this->logger = Logger::getInstance();
		$this->cache = new Cache();
		
		// Verify API credentials immediately
		$this->verifyConnection();
	}
	
	private function verifyConnection() {
		$ch = curl_init('https://api.openai.com/v1/models');
		curl_setopt_array($ch, [
			CURLOPT_RETURNTRANSFER => true,
			CURLOPT_HTTPHEADER => [
				"Authorization: Bearer $this->apiKey"
			],
			CURLOPT_TIMEOUT => 15
		]);
		
		$result = curl_exec($ch);
		$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
		curl_close($ch);
		
		if ($httpCode !== 200) {
			$this->logger->log("OpenAI API connection test failed with status: $httpCode", 'ERROR');
			throw new RuntimeException("Failed to verify OpenAI API connection");
		}
		
		$this->logger->log("OpenAI API connection verified successfully");
	}

	public function getStructuredStaffData($content) {
		$cacheKey = 'staff_' . md5($content);
		
		if ($cached = $this->cache->get($cacheKey)) {
			$this->logger->log("Using cached staff data");
			return $cached;
		}

		$this->logger->log("Initiating OpenAI API call for staff data");
		$prompt = "Extract all staff members from the following unstructured text and return as JSON. Include fields: full_name, title, phone, email_address, sport_department. IMPORTANT: Keep the exact sport/department names as they appear in the headings (e.g., 'Baseball Coaches' not just 'Baseball'). Return ONLY the JSON without any additional text or markdown formatting. Remove any tabs or special characters.";
		
		$response = $this->makeRequest($prompt, $content);
		$this->logger->log("Caching staff data response");
		$this->cache->set($cacheKey, $response);
		
		return $response;
	}

	public function getSchoolInfo($schoolName) {
		$cacheKey = 'school_' . md5($schoolName);
		
		if ($cached = $this->cache->get($cacheKey)) {
			$this->logger->log("Found cached school info - validating structure");
			
			// Validate cached data has complete structure before using
			$decodedCache = json_decode($cached, true);
			if ($this->validateSchoolInfoResponse($decodedCache)) {
				$this->logger->log("Using validated cached school info");
				return $cached;
			} else {
				$this->logger->log("Cached school info invalid or empty - fetching fresh data");
			}
		}
	
		$this->logger->log("Initiating OpenAI API call for school info");
		$prompt = "Analyze and return information about {$schoolName} formatted as a JSON object with these fields:
		- school_colors: array of the school's official colors
		- school_divisions: array of athletic divisions (e.g. NCAA Division I, II, III, NAIA)
		- school_conferences: array of athletic conferences
		- school_address: object containing street_1, street_2, street_3, city, county, state, zipcode, country
		- school_level: string indicating whether the school is a senior college or a junior college
		- school_website: string of the school's main website URL
		- athletics_website: string of the school's athletics URL
		- athletics_phone: string of the school's athletics department phone number
		- mascot: string of the school's athletics mascot name
		- school_type: string indicating whether the school is a public or private institution
		- school_enrollment: string containing the EXACT, SPECIFIC most recent enrollment number - do not round or approximate. If the most recent enrollment is 507, return '507', not '500'
		- football_division: string MUST BE EXACTLY one of these three values: 'FBS', 'FCS', or 'Neither'
		
		Return only valid JSON. Use research to find accurate information. Include all known data.
		For the football_division field, if the school competes in NCAA Division I Football Bowl Subdivision (FBS), return 'FBS'.
		If the school competes in NCAA Division I Football Championship Subdivision (FCS), return 'FCS'.
		For all other cases (including no football program), return 'Neither'.
		For any other truly unknown values, use empty arrays [] or empty strings.";
		
		$response = $this->makeRequest($prompt, $schoolName);
		
		// Validate response before caching
		$decodedResponse = json_decode($response, true);
		if (!$this->validateSchoolInfoResponse($decodedResponse)) {
			$this->logger->log("OpenAI returned invalid/empty school info - retrying with modified prompt");
			// Retry once with a modified prompt
			$retryPrompt = "Research and provide specific information about {$schoolName} including: 
				1. Official school colors
				2. Athletic division affiliations (NCAA/NAIA)
				3. Athletic conference memberships
				4. Complete mailing address, including street, city, county, state, zipcode, and country
				5. School Level - Either Senior College or Junior College
				6. School main website URL
				7. Athletics website URL
				8. Athletic department phone number
				9. Athletics mascot name
				10. School Type - Either a public or private institution
				11. School enrollment - provide the EXACT, SPECIFIC most recent enrollment number without rounding or approximating (e.g., if enrollment is 507, return '507', not '500')
				12. Football Division - EXACTLY one of: 'FBS', 'FCS', or 'Neither'
				Format as JSON with school_colors (array), school_divisions (array), 
				school_conferences (array), school_address (object), school_level (string), 
				school_website (string), athletics_website (string), athletics_phone (string), 
				mascot (string), school_type (string), school_enrollment (string), and 
				football_division (string) fields.";
			$response = $this->makeRequest($retryPrompt, $schoolName);
		}
		
		$this->logger->log("Caching validated school info response");
		$this->cache->set($cacheKey, $response);
		
		return $response;
	}
	
	private function validateSchoolInfoResponse($data) {
		// Check if we have at least one non-empty field
		$hasData = false;
		
		if (!is_array($data)) {
			return false;
		}
		
		// Check required structure exists
		if (!isset($data['school_colors']) || !isset($data['school_divisions']) || 
			!isset($data['school_conferences']) || !isset($data['school_address']) || !isset($data['school_level']) || !isset($data['school_type']) || !isset($data['school_website']) || !isset($data['athletics_website']) || !isset($data['athletics_phone']) || !isset($data['mascot']) || !isset($data['school_enrollment'])) {
			return false;
		}
		
		// Check if any arrays have data
		if (!empty($data['school_colors'])) $hasData = true;
		if (!empty($data['school_divisions'])) $hasData = true;
		if (!empty($data['school_conferences'])) $hasData = true;
		
		// Check if address has any non-empty fields
		foreach ($data['school_address'] as $field => $value) {
			if (!empty($value)) {
				$hasData = true;
				break;
			}
		}
		
		return $hasData;
	}

	protected function makeRequest($prompt, $content) {
		$maxRetries = 3;
		$currentTry = 0;
		
		while ($currentTry < $maxRetries) {
			try {
				$this->logger->log("OpenAI API attempt " . ($currentTry + 1) . " of $maxRetries");
				
				$data = [
					'model' => 'gpt-4o-mini',
					'messages' => [
						['role' => 'system', 'content' => 'You are a data analyst converting unstructured text to structured data. Return only JSON without any markdown formatting or additional text.'],
						['role' => 'user', 'content' => $prompt . $content]
					],
					'temperature' => 0.0
				];
	
				$this->logger->log("Request payload size: " . strlen(json_encode($data)) . " bytes");
				$this->logger->log("Raw prompt + content: " . $prompt . $content);
	
				// Initialize CURL with error buffer
				$ch = curl_init($this->apiUrl);
				$errorBuffer = fopen('php://temp', 'w+');
				
				curl_setopt_array($ch, [
					CURLOPT_RETURNTRANSFER => true,
					CURLOPT_HTTPHEADER => [
						'Content-Type: application/json',
						"Authorization: Bearer $this->apiKey"
					],
					CURLOPT_POST => true,
					CURLOPT_POSTFIELDS => json_encode($data),
					CURLOPT_CONNECTTIMEOUT => $this->connectionTimeout,
					CURLOPT_TIMEOUT => $this->totalTimeout,
					CURLOPT_STDERR => $errorBuffer,
					CURLOPT_VERBOSE => true,
					CURLOPT_DNS_USE_GLOBAL_CACHE => false,
					CURLOPT_FRESH_CONNECT => true,
					CURLOPT_FORBID_REUSE => true
				]);
	
				$this->logger->log("Starting OpenAI API request");
				$startTime = microtime(true);
				
				$response = curl_exec($ch);
				$endTime = microtime(true);
				
				// Get CURL debug info
				rewind($errorBuffer);
				$debugOutput = stream_get_contents($errorBuffer);
				fclose($errorBuffer);
				
				$this->logger->log("CURL Debug Output: " . $debugOutput);
				
				if (curl_errno($ch)) {
					throw new RuntimeException("CURL Error (" . curl_errno($ch) . "): " . curl_error($ch));
				}
	
				$httpCode = curl_getinfo($ch, CURLINFO_HTTP_CODE);
				$this->logger->log("Request completed in " . round($endTime - $startTime, 2) . " seconds with status code: " . $httpCode);
				
				curl_close($ch);
	
				if ($httpCode !== 200) {
					throw new RuntimeException("OpenAI API returned status code: $httpCode");
				}
	
				// Log raw response for debugging
				$this->logger->log("Raw API Response: " . $response);
	
				$decoded = json_decode($response, true);
				if ($decoded === null) {
					throw new RuntimeException("JSON decode failed: " . json_last_error_msg());
				}
	
				// Log decoded response structure
				$this->logger->log("Decoded API Response structure: " . print_r($decoded, true));
	
				if (!isset($decoded['choices'][0]['message']['content'])) {
					throw new RuntimeException("Unexpected API response structure");
				}
	
				$content = $decoded['choices'][0]['message']['content'];
				
				// Log the final extracted content
				$this->logger->log("Extracted content from API response: " . $content);
	
				return $content;
	
			} catch (Exception $e) {
				$currentTry++;
				$this->logger->log("API call failed: " . $e->getMessage(), 'ERROR');
				
				if ($currentTry >= $maxRetries) {
					throw new RuntimeException("All API retries failed: " . $e->getMessage());
				}
				
				$waitTime = pow(2, $currentTry);
				$this->logger->log("Waiting {$waitTime} seconds before retry...");
				sleep($waitTime);
			}
		}
	}
}

class ContentChunker {
	private $logger;
	private $maxChunkSize = 16384; // 16KB per chunk
	private $maxStaffPerChunk = 20;
	private $totalTokenLimit = 4000; // GPT-3.5/4 context limit
	
	public function __construct(Logger $logger) {
		$this->logger = $logger;
	}
	
	public function chunkContent($content) {
		$this->logger->log("Starting content chunking");
		$this->logger->log("Content size: " . strlen($content) . " bytes");
		
		// Log a sample of the content for debugging
		$this->logger->log("Content sample (first 200 chars): " . substr($content, 0, 200));
		
		// For small content, return as single chunk
		if (strlen($content) <= $this->totalTokenLimit) {
			$this->logger->log("Content is small enough for a single chunk");
			return [$this->normalizeChunk($content)];
		}
		
		// First try to split by major department headers
		$departments = $this->splitIntoDepartments($content);
		$this->logger->log("Initial department split count: " . count($departments));
		
		if (empty($departments)) {
			// If no departments found, try basic chunking
			$this->logger->log("No departments found, using basic chunking");
			return $this->basicChunk($content);
		}
		
		$chunks = [];
		$currentChunk = '';
		$staffCount = 0;
		
		foreach ($departments as $department) {
			$this->logger->log("Processing department chunk of size: " . strlen($department));
			
			// Estimate tokens (rough approximation)
			$estimatedNewTokens = $this->estimateTokens($currentChunk . $department);
			$estimatedStaffCount = $this->estimateStaffCount($department);
			
			$this->logger->log("Estimated tokens: $estimatedNewTokens, Estimated staff: $estimatedStaffCount");
			
			if (($estimatedNewTokens > $this->totalTokenLimit || 
				 $staffCount + $estimatedStaffCount > $this->maxStaffPerChunk) 
				&& !empty($currentChunk)) {
				$chunks[] = $this->normalizeChunk($currentChunk);
				$currentChunk = '';
				$staffCount = 0;
			}
			
			$currentChunk .= (!empty($currentChunk) ? "\n\n" : '') . $department;
			$staffCount += $estimatedStaffCount;
		}
		
		if (!empty($currentChunk)) {
			$chunks[] = $this->normalizeChunk($currentChunk);
		}
		
		// If any chunk is still too large, split by staff members
		$finalChunks = [];
		foreach ($chunks as $i => $chunk) {
			$this->logger->log("Processing chunk $i, size: " . strlen($chunk));
			if (strlen($chunk) > $this->maxChunkSize || 
				$this->estimateStaffCount($chunk) > $this->maxStaffPerChunk) {
				$this->logger->log("Chunk $i needs further splitting");
				$finalChunks = array_merge(
					$finalChunks, 
					$this->splitByStaffMembers($chunk)
				);
			} else {
				$finalChunks[] = $chunk;
			}
		}
		
		$this->logger->log("Created " . count($finalChunks) . " final chunks");
		foreach ($finalChunks as $i => $chunk) {
			$this->logger->log("Chunk $i size: " . strlen($chunk) . " bytes");
		}
		
		return $finalChunks;
	}
	
	private function basicChunk($content) {
		// Simple chunking by size if no departments found
		$chunks = [];
		$parts = str_split($content, $this->maxChunkSize);
		foreach ($parts as $part) {
			if (trim($part) !== '') {
				$chunks[] = $this->normalizeChunk($part);
			}
		}
		$this->logger->log("Basic chunking created " . count($chunks) . " chunks");
		return $chunks;
	}
	
	private function splitIntoDepartments($content) {
		// Enhanced department header patterns - fixed regex syntax
		$patterns = [
			'(?:\r?\n|\A)(?:[A-Z][A-Za-z\s]+(?:Athletics|Sports|Department|Staff|Team|Office|Personnel)).*?(?=\r?\n|$)',
			'(?:\r?\n|\A)(?:Administration|Staff|Coaches|Faculty|Personnel).*?(?=\r?\n|$)',
			'(?:\r?\n|\A)(?:Baseball|Basketball|Football|Soccer|Tennis|Golf|Swimming|Track|Volleyball|Softball).*?(?=\r?\n|$)',
			'(?:\r?\n|\A)(?:[A-Z][A-Z\s\']+).*?(?=\r?\n|$)'
		];
		
		$this->logger->log("Attempting to split content by department patterns");
		
		// Create the combined pattern correctly
		$combinedPattern = '/' . implode('|', array_map(function($p) {
			return '(' . $p . ')';
		}, $patterns)) . '/i';
		
		try {
			$sections = preg_split(
				$combinedPattern,
				$content,
				-1,
				PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY
			);
			
			if ($sections === false) {
				$this->logger->log("Error in preg_split: " . preg_last_error_msg());
				return $this->basicChunk($content);
			}
			
			$this->logger->log("Initial section split count: " . count($sections));
			
			$departments = [];
			$currentDept = '';
			
			foreach ($sections as $section) {
				$trimmedSection = trim($section);
				if (preg_match('/^(?:[A-Z][A-Za-z\s]+(?:Athletics|Sports|Department|Staff|Team|Office|Personnel)|Administration|Staff|Coaches|Faculty|Baseball|Basketball|Football|Soccer|Tennis|Golf|Swimming|Track|Volleyball|Softball)/i', $trimmedSection)) {
					if (!empty($currentDept)) {
						$departments[] = trim($currentDept);
					}
					$currentDept = $section;
				} else {
					$currentDept .= "\n" . $section;
				}
			}
			
			if (!empty($currentDept)) {
				$departments[] = trim($currentDept);
			}
			
			$this->logger->log("Found " . count($departments) . " departments");
			foreach ($departments as $i => $dept) {
				$this->logger->log("Department $i size: " . strlen($dept) . " bytes");
			}
			
			if (empty($departments)) {
				$this->logger->log("No departments found, falling back to basic chunking");
				return $this->basicChunk($content);
			}
			
			return $departments;
			
		} catch (Exception $e) {
			$this->logger->log("Error in splitIntoDepartments: " . $e->getMessage());
			return $this->basicChunk($content);
		}
	}
	
	private function splitByStaffMembers($content) {
		$chunks = [];
		$currentChunk = '';
		$staffCount = 0;
		
		// Split content into individual staff member blocks
		$staffBlocks = preg_split(
			'/(?=(?:\r?\n|\A)(?:[A-Z][a-z]+\s+){1,3}(?:Jr\.|Sr\.|III|IV|V|\(?\d{4}\)?)?$)/m',
			$content,
			-1,
			PREG_SPLIT_NO_EMPTY
		);
		
		$this->logger->log("Split content into " . count($staffBlocks) . " staff blocks");
		
		foreach ($staffBlocks as $block) {
			if ($staffCount >= $this->maxStaffPerChunk || 
				$this->estimateTokens($currentChunk . $block) > $this->totalTokenLimit) {
				if (!empty($currentChunk)) {
					$chunks[] = $this->normalizeChunk($currentChunk);
					$currentChunk = '';
					$staffCount = 0;
				}
			}
			
			$currentChunk .= (!empty($currentChunk) ? "\n\n" : '') . $block;
			$staffCount++;
		}
		
		if (!empty($currentChunk)) {
			$chunks[] = $this->normalizeChunk($currentChunk);
		}
		
		$this->logger->log("Created " . count($chunks) . " chunks from staff blocks");
		
		return $chunks;
	}
	
	private function estimateTokens($text) {
		// Rough token estimation (average English words are ~4 characters)
		return (int)(strlen($text) / 4);
	}
	
	public function estimateStaffCount($text) {
		// Look for common patterns indicating staff members
		$patterns = [
			'/(?:\r?\n|\A)[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3}(?:Jr\.|Sr\.|III|IV|V|\(?\d{4}\)?)?$/m',
			'/(?:Head|Assistant|Associate)\s+(?:Coach|Director|Administrator)/i',
			'/(?:Email|Phone|Extension):/i'
		];
		
		$maxCount = 0;
		foreach ($patterns as $pattern) {
			$count = preg_match_all($pattern, $text);
			$maxCount = max($maxCount, $count);
		}
		
		return $maxCount;
	}
	
	private function normalizeChunk($chunk) {
		// Clean up the chunk formatting
		$chunk = trim($chunk);
		$chunk = preg_replace('/\R{3,}/', "\n\n", $chunk); // Replace multiple newlines
		$chunk = preg_replace('/[ \t]+/', ' ', $chunk); // Normalize spaces
		return $chunk;
	}
}

class ProcessPool {
	private $maxProcesses;
	private $logger;
	private $memoryLimit;
	private $processTimeout;
	
	public function __construct(Logger $logger) {
		$this->logger = $logger;
		
		// Get PHP memory limit
		$memoryLimit = $this->getMemoryLimit();
		// Calculate safe number of processes based on memory
		$maxProcessesByMemory = floor($memoryLimit / (256 * 1024 * 1024)); // 256MB per process
		
		// Instead of using shell_exec to get CPU cores, use a conservative default
		$cpuCores = 4; // Conservative default
		
		// Use the most conservative limit
		$this->maxProcesses = min(
			MAX_PARALLEL_PROCESSES,
			$maxProcessesByMemory,
			$cpuCores - 1, // Leave one core free
			6 // Hard maximum
		);
		
		// Leave 20% memory buffer
		$this->memoryLimit = $this->getMemoryLimit() * 0.8;
		$this->processTimeout = 300; // 5 minutes max per process
		
		$this->logger->log("Process pool initialized with {$this->maxProcesses} workers");
	}
	
	public function processChunks(array $chunks, callable $processor) {
		$totalChunks = count($chunks);
		$activeProcesses = [];
		$results = [];
		$completed = 0;
		
		$this->logger->log("Starting to process $totalChunks chunks");
		
		// Create temp directory for process communication
		$tempDir = sys_get_temp_dir() . '/parallel_' . uniqid();
		mkdir($tempDir);
		
		try {
			while ($completed < $totalChunks) {
				// Start new processes if we have capacity and chunks
				while (count($activeProcesses) < $this->maxProcesses && !empty($chunks)) {
					$chunk = array_shift($chunks);
					$resultFile = $tempDir . '/result_' . count($activeProcesses) . '.json';
					
					$pid = pcntl_fork();
					
					if ($pid == -1) {
						throw new RuntimeException("Failed to fork process");
					} elseif ($pid) {
						// Parent process
						$activeProcesses[$pid] = [
							'start_time' => time(),
							'result_file' => $resultFile
						];
						$this->logger->log("Started process $pid");
					} else {
						// Child process
						try {
							$result = $processor($chunk);
							file_put_contents($resultFile, json_encode($result));
							exit(0);
						} catch (Exception $e) {
							$this->logger->log("Child process error: " . $e->getMessage(), 'ERROR');
							exit(1);
						}
					}
				}
				
				// Check for completed processes
				foreach ($activeProcesses as $pid => $process) {
					$status = 0;
					$res = pcntl_waitpid($pid, $status, WNOHANG);
					
					// Process completed
					if ($res === $pid) {
						if (pcntl_wexitstatus($status) === 0 && file_exists($process['result_file'])) {
							$results[] = json_decode(file_get_contents($process['result_file']), true);
							unlink($process['result_file']);
						}
						unset($activeProcesses[$pid]);
						$completed++;
						$this->logger->log("Process $pid completed ($completed/$totalChunks done)");
					} else {
						// Check for timeout
						if (time() - $process['start_time'] > $this->processTimeout) {
							posix_kill($pid, SIGTERM);
							$this->logger->log("Process $pid timed out and was terminated", 'WARNING');
							unset($activeProcesses[$pid]);
							$completed++;
						}
					}
				}
				
				// Avoid busy waiting
				if (!empty($activeProcesses)) {
					usleep(100000); // 100ms
				}
				
				// Check memory usage
				if ($this->getMemoryUsage() > $this->memoryLimit) {
					throw new RuntimeException("Memory limit exceeded");
				}
			}
			
			return $results;
			
		} catch (Exception $e) {
			$this->logger->log("Error in process pool: " . $e->getMessage(), 'ERROR');
			// Kill all active processes
			foreach ($activeProcesses as $pid => $process) {
				posix_kill($pid, SIGTERM);
				@unlink($process['result_file']);
			}
			throw $e;
		} finally {
			// Cleanup
			foreach ($activeProcesses as $pid => $process) {
				posix_kill($pid, SIGTERM);
				@unlink($process['result_file']);
			}
			@rmdir($tempDir);
		}
	}
	
	private function getMemoryLimit() {
		$limit = ini_get('memory_limit');
		if ($limit === '-1') return PHP_INT_MAX;
		
		$value = (int)$limit;
		$unit = strtolower(substr($limit, -1));
		
		switch ($unit) {
			case 'g': $value *= 1024;
			case 'm': $value *= 1024;
			case 'k': $value *= 1024;
		}
		
		return $value;
	}
	
	private function getMemoryUsage() {
		return memory_get_usage(true);
	}
}

class EnhancedOpenAiClient extends OpenAiClient {
	private $chunker;
	private $pool;
	private $cache;
	protected $logger; // Add this line
	
	public function __construct($apiKey, Logger $logger) {
		parent::__construct($apiKey);
		$this->logger = $logger;
		$this->chunker = new ContentChunker($logger);
		$this->pool = new ProcessPool($logger);
		$this->cache = new Cache();
	}
	
	public function getStructuredStaffData($content) {
		$cacheKey = 'staff_' . md5($content);
		
		if ($cached = $this->cache->get($cacheKey)) {
			$this->logger->log("Using cached staff data");
			return $cached;
		}
		
		// Check if content needs chunking
		if (strlen($content) > 16384 || $this->chunker->estimateStaffCount($content) > 20) {
			return $this->processLargeContent($content);
		}
		
		return $this->processSingleChunk($content);
	}
	
	private function processLargeContent($content) {
		$chunks = $this->chunker->chunkContent($content);
		$this->logger->log("Processing " . count($chunks) . " chunks in parallel");
		
		$results = $this->pool->processChunks($chunks, function($chunk) {
			return $this->processSingleChunk($chunk);
		});
		
		// Merge results from all chunks
		$mergedStaff = [];
		foreach ($results as $result) {
			$staffData = json_decode($result, true);
			if (isset($staffData['staff_members'])) {
				$mergedStaff = array_merge($mergedStaff, $staffData['staff_members']);
			}
		}
		
		// Deduplicate staff members based on name and department
		$uniqueStaff = [];
		foreach ($mergedStaff as $staff) {
			$key = $staff['full_name'] . '|' . ($staff['sport_department'] ?? '');
			if (!isset($uniqueStaff[$key])) {
				$uniqueStaff[$key] = $staff;
			} else {
				// Merge any additional information
				foreach ($staff as $field => $value) {
					if (!empty($value) && empty($uniqueStaff[$key][$field])) {
						$uniqueStaff[$key][$field] = $value;
					}
				}
			}
		}
		
		$finalResult = json_encode(['staff_members' => array_values($uniqueStaff)]);
		$this->cache->set('staff_' . md5($content), $finalResult);
		
		return $finalResult;
	}
	
	private function processSingleChunk($chunk) {
		$prompt = $this->buildPrompt();
		
		try {
			$response = $this->makeRequest($prompt, $chunk);
			$decoded = json_decode($response, true);
			
			if (!$decoded || !isset($decoded['staff_members'])) {
				throw new RuntimeException("Invalid API response format");
			}
			
			return $response;
			
		} catch (Exception $e) {
			$this->logger->log("Error processing chunk: " . $e->getMessage(), 'ERROR');
			throw $e;
		}
	}
	
	private function buildPrompt() {
		return <<<EOT
Extract staff members from the following text section. Return a JSON object with a "staff_members" array containing objects with these fields:
- full_name (required)
- title
- phone
- email_address
- sport_department

IMPORTANT:
1. Keep exact department names as they appear
2. Include all contact information found
3. Return ONLY valid JSON without any additional text
4. Combine multiple titles/roles for the same person
5. Include staff members even if some fields are missing
6. Use consistent formatting for phone numbers
7. Normalize email addresses to lowercase
8. Remove any tabs or special characters

Example format:
{
  "staff_members": [
	{
	  "full_name": "John Smith",
	  "title": "Head Baseball Coach, Recruiting Coordinator",
	  "phone": "555-123-4567",
	  "email_address": "jsmith@example.edu",
	  "sport_department": "Baseball"
	}
  ]
}
EOT;
	}
}

class DbClient {
	private $conn;
	private $logger;
	use StringSanitizerTrait;

	public function __construct() {
		$this->logger = Logger::getInstance();
		$this->conn = $this->getDbConnection();
	}
	
	/**
	 * Convert a string like '16M' to bytes (16777216)
	 */
	private function convertToBytes($value) {
		$value = trim($value);
		$unit = strtolower(substr($value, -1));
		$value = (int) $value;
		
		switch ($unit) {
			case 'g': $value *= 1024;
			case 'm': $value *= 1024;
			case 'k': $value *= 1024;
		}
		
		return $value;
	}

	private function getDbConnection() {
		// Database credentials should be in your .env file
		$host = getenv('DB_HOST') ?: 'localhost';
		$user = getenv('DB_USER') ?: 'collegesportsdir';
		$password = getenv('DB_PASSWORD') ?: '';
		$database = getenv('DB_NAME') ?: 'collegesportsdir';
		
		$conn = new mysqli($host, $user, $password, $database);
		if ($conn->connect_error) {
			$this->logger->log("Database connection failed: " . $conn->connect_error, 'ERROR');
			throw new RuntimeException("Database connection failed");
		}
		
		// Set charset to ensure proper handling of special characters
		$conn->set_charset("utf8mb4");
		
		// Set MUCH higher timeouts - this is key to preventing "server has gone away"
		$conn->query("SET SESSION wait_timeout=1800"); // 30 minutes
		$conn->query("SET SESSION interactive_timeout=1800"); // 30 minutes
		$conn->query("SET SESSION net_read_timeout=600"); // 10 minutes
		$conn->query("SET SESSION net_write_timeout=600"); // 10 minutes
		
		return $conn;
	}
	
	public function checkConnection() {
		// Check if connection is still alive
		try {
			// Try ping, but be prepared for it to fail
			if (!$this->conn->ping()) {
				$this->logger->log("Database connection lost, reconnecting...");
				// Close the dead connection
				try {
					$this->conn->close();
				} catch (Exception $e) {
					// Ignore errors closing a dead connection
					$this->logger->log("Notice: Error closing dead connection: " . $e->getMessage(), 'DEBUG');
				}
				// Establish a new connection
				$this->conn = $this->getDbConnection();
				$this->logger->log("Database reconnection successful");
			}
		} catch (Exception $e) {
			$this->logger->log("Connection check failed: " . $e->getMessage(), 'WARNING');
			// Try to create a new connection anyway
			try {
				$this->conn = $this->getDbConnection();
				$this->logger->log("Force-created new database connection");
			} catch (Exception $innerEx) {
				$this->logger->log("CRITICAL: Failed to reconnect: " . $innerEx->getMessage(), 'ERROR');
				throw $innerEx;
			}
		}
	}
	
	// Database keep alive method
	public function keepConnectionAlive() {
		$this->checkConnection();
		$this->conn->query("SELECT 1");
		$this->logger->log("Sent heartbeat query to keep database connection alive", 'DEBUG');
	}
	
	// Add this method to the DbClient class
	public function checkConnectionDeeper() {
		$this->logger->log("Running deep database connection check...");
		
		try {
			// First try a simple ping
			if (!$this->conn->ping()) {
				$this->logger->log("Ping failed, reconnecting database");
				$this->conn->close();
				$this->conn = $this->getDbConnection();
				return;
			}
			
			// Try a simple query to really confirm connection
			$result = $this->conn->query("SELECT 1 as test");
			if (!$result) {
				$this->logger->log("Test query failed, reconnecting database");
				$this->conn->close();
				$this->conn = $this->getDbConnection();
				return;
			}
			
			$row = $result->fetch_assoc();
			if (!isset($row['test']) || $row['test'] != 1) {
				$this->logger->log("Test query returned unexpected result, reconnecting database");
				$this->conn->close();
				$this->conn = $this->getDbConnection();
				return;
			}
			
			$this->logger->log("Deep connection check passed");
		} catch (Throwable $e) {
			$this->logger->log("Exception during connection check: " . $e->getMessage(), 'ERROR');
			try {
				$this->conn->close();
			} catch (Throwable $e2) {
				// Ignore errors on close
			}
			$this->conn = $this->getDbConnection();
		}
	}
	
	// Also add this to your WebhookHandler class to create a new connection if needed
	private function resetDatabaseConnection() {
		$this->logger->log("RESETTING DATABASE CONNECTION");
		
		try {
			$this->dbClient = new DbClient();
			$this->logger->log("Successfully created new database connection");
		} catch (Exception $e) {
			$this->logger->log("Failed to create new database connection: " . $e->getMessage(), 'ERROR');
			// Wait and try once more
			sleep(5);
			$this->dbClient = new DbClient();
		}
	}
	
	public function executeTransaction($callback) {
		try {
			$this->checkConnectionDeeper();
			$this->logger->log("Starting transaction with verified connection");
			
			$this->conn->begin_transaction();
			
			$result = $callback($this);
			
			$this->conn->commit();
			$this->logger->log("Transaction committed successfully");
			
			return $result;
		} catch (Throwable $e) {
			if ($this->conn) {
				try {
					$this->conn->rollback();
					$this->logger->log("Transaction rolled back due to error");
				} catch (Throwable $rollbackError) {
					$this->logger->log("Could not rollback transaction: " . $rollbackError->getMessage(), 'ERROR');
				}
			}
			
			throw $e;
		}
	}
	
	public function directQuery($sql) {
		$this->checkConnection();
		return $this->conn->query($sql);
	}
	
	// Add this after checkConnection() and keepConnectionAlive() methods
	private function executeWithRetry($callback, $maxRetries = 3) {
		$attempts = 0;
		
		while ($attempts < $maxRetries) {
			try {
				$this->checkConnection();
				return $callback();
			} catch (mysqli_sql_exception $e) {
				$attempts++;
				if (strpos($e->getMessage(), 'server has gone away') !== false || 
				   strpos($e->getMessage(), 'Lost connection') !== false) {
				   $this->logger->log("Database connection lost, retrying ({$attempts}/{$maxRetries})...");
				   $this->conn = $this->getDbConnection(); // Force reconnection
				   
				   if ($attempts >= $maxRetries) {
					   throw $e;
				   }
				   
				   // Exponential backoff
				   sleep(pow(2, $attempts));
			   } else {
				   throw $e; // Re-throw other exceptions
			   }
			}
		}
	}
	
	public function processSchool($schoolInfo) {
		$this->logger->log("Processing school: " . $schoolInfo['school_name']);
		
		// First, check if connection is still alive
		$this->checkConnection();
		
		// Then, validate the school info
		if (empty($schoolInfo['school_name'])) {
			throw new RuntimeException("School name is required");
		}
		
		// Check if school exists by name
		$stmt = $this->conn->prepare("SELECT id FROM csd_schools WHERE school_name = ?");
		$stmt->bind_param("s", $schoolInfo['school_name']);
		$stmt->execute();
		$result = $stmt->get_result();
		
		if ($result->num_rows == 0) {
			// Insert new school
			$this->logger->log("Creating new school record");
			
			$query = "INSERT INTO csd_schools (
				school_name, street_address_line_1, street_address_line_2, street_address_line_3, 
				city, state, zipcode, country, school_colors, school_divisions, school_conferences, 
				school_level, school_website, athletics_website, school_type, county, athletics_phone, 
				mascot, school_enrollment, football_division, date_created, date_updated
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NOW(), NOW())";
			
			$stmt = $this->conn->prepare($query);
			$stmt->bind_param(
				"ssssssssssssssssssss",
				$schoolInfo['school_name'],
				$schoolInfo['school_address']['street_1'],
				$schoolInfo['school_address']['street_2'],
				$schoolInfo['school_address']['street_3'],
				$schoolInfo['school_address']['city'],
				$schoolInfo['school_address']['state'],
				$schoolInfo['school_address']['zipcode'],
				$schoolInfo['school_address']['country'],
				$colors,
				$divisions,
				$conferences,
				$schoolInfo['school_level'],
				$schoolInfo['school_website'],
				$schoolInfo['athletics_website'],
				$schoolInfo['school_type'],
				$schoolInfo['school_address']['county'],
				$schoolInfo['athletics_phone'],
				$schoolInfo['mascot'],
				$schoolInfo['school_enrollment'],
				$schoolInfo['football_division']
			);
			
			// Convert arrays to comma-separated strings
			$colors = is_array($schoolInfo['school_colors']) ? 
				implode(', ', array_filter($schoolInfo['school_colors'])) : 
				$schoolInfo['school_colors'];
				
			$divisions = is_array($schoolInfo['school_divisions']) ? 
				implode(', ', array_filter($schoolInfo['school_divisions'])) : 
				$schoolInfo['school_divisions'];
				
			$conferences = is_array($schoolInfo['school_conferences']) ? 
				implode(', ', array_filter($schoolInfo['school_conferences'])) : 
				$schoolInfo['school_conferences'];
			
			if (!$stmt->execute()) {
				throw new RuntimeException("Failed to create school record: " . $stmt->error);
			}
			
			$schoolId = $this->conn->insert_id;
			$stmt->close();
		} else {
			// Update existing school
			$row = $result->fetch_assoc();
			$schoolId = $row['id'];
			$stmt->close();
			
			$this->logger->log("Found existing school record: $schoolId");
			
			// Get current record
			$stmt = $this->conn->prepare("SELECT * FROM csd_schools WHERE id = ?");
			$stmt->bind_param("i", $schoolId);
			$stmt->execute();
			$current = $stmt->get_result()->fetch_assoc();
			$stmt->close();
			
			// Convert arrays to comma-separated strings
			$colors = is_array($schoolInfo['school_colors']) ? 
				implode(', ', array_filter($schoolInfo['school_colors'])) : 
				$schoolInfo['school_colors'];
				
			$divisions = is_array($schoolInfo['school_divisions']) ? 
				implode(', ', array_filter($schoolInfo['school_divisions'])) : 
				$schoolInfo['school_divisions'];
				
			$conferences = is_array($schoolInfo['school_conferences']) ? 
				implode(', ', array_filter($schoolInfo['school_conferences'])) : 
				$schoolInfo['school_conferences'];
			
			// Only update if data has changed
			if ($this->hasSchoolDataChanged($current, $schoolInfo)) {
				$this->logger->log("School data has changed, updating");
				
				$query = "UPDATE csd_schools SET 
					street_address_line_1 = ?, street_address_line_2 = ?, street_address_line_3 = ?, 
					city = ?, state = ?, zipcode = ?, country = ?, school_colors = ?, 
					school_divisions = ?, school_conferences = ?, school_level = ?, 
					school_website = ?, athletics_website = ?, school_type = ?, 
					county = ?, athletics_phone = ?, mascot = ?, school_enrollment = ?, 
					football_division = ?, date_updated = NOW()
				WHERE id = ?";
				
				$stmt = $this->conn->prepare($query);
				$stmt->bind_param(
					"sssssssssssssssssssi",
					$schoolInfo['school_address']['street_1'],
					$schoolInfo['school_address']['street_2'],
					$schoolInfo['school_address']['street_3'],
					$schoolInfo['school_address']['city'],
					$schoolInfo['school_address']['state'],
					$schoolInfo['school_address']['zipcode'],
					$schoolInfo['school_address']['country'],
					$colors,
					$divisions,
					$conferences,
					$schoolInfo['school_level'],
					$schoolInfo['school_website'],
					$schoolInfo['athletics_website'],
					$schoolInfo['school_type'],
					$schoolInfo['school_address']['county'],
					$schoolInfo['athletics_phone'],
					$schoolInfo['mascot'],
					$schoolInfo['school_enrollment'],
					$schoolInfo['football_division'],
					$schoolId
				);
				
				if (!$stmt->execute()) {
					throw new RuntimeException("Failed to update school record: " . $stmt->error);
				}
				
				$stmt->close();
			} else {
				$this->logger->log("No changes needed for school record");
			}
		}
		
		$this->logger->log("Returning school ID: $schoolId");
		return $schoolId;
	}
	
	private function hasSchoolDataChanged($current, $new) {
		$fields = [
			'street_address_line_1' => 'school_address.street_1',
			'street_address_line_2' => 'school_address.street_2', 
			'street_address_line_3' => 'school_address.street_3',
			'city' => 'school_address.city',
			'state' => 'school_address.state',
			'county' => 'school_address.county',
			'zipcode' => 'school_address.zipcode',
			'country' => 'school_address.country',
			'school_level' => 'school_level',
			'school_website' => 'school_website',
			'athletics_website' => 'athletics_website',
			'athletics_phone' => 'athletics_phone',
			'mascot' => 'mascot',
			'school_type' => 'school_type',
			'school_enrollment' => 'school_enrollment',
			'football_division' => 'football_division'
		];
		
		foreach ($fields as $dbField => $infoField) {
			$parts = explode('.', $infoField);
			$newValue = count($parts) > 1 ? $new[$parts[0]][$parts[1]] : $new[$infoField];
			
			if (($current[$dbField] ?? '') !== ($newValue ?? '')) {
				return true;
			}
		}
		
		// Also check arrays that were converted to strings
		$arrayFields = [
			'school_colors' => 'school_colors',
			'school_divisions' => 'school_divisions',
			'school_conferences' => 'school_conferences'
		];
		
		foreach ($arrayFields as $dbField => $infoField) {
			$currentValue = $current[$dbField] ?? '';
			$newValue = is_array($new[$infoField]) ? 
				implode(', ', array_filter($new[$infoField])) : 
				$new[$infoField];
				
			if ($currentValue !== $newValue) {
				return true;
			}
		}
		
		return false;
	}

	public function getStaffMembersForSchool($schoolId) {
		$this->logger->log("Retrieving staff members for school ID: $schoolId");
		
		// First, check if connection is still alive
		$this->checkConnection();
		
		$query = "
			SELECT s.* 
			FROM csd_staff s
			JOIN csd_school_staff ss ON s.id = ss.staff_id
			WHERE ss.school_id = ?
			ORDER BY s.full_name
		";
		
		$stmt = $this->conn->prepare($query);
		$stmt->bind_param("i", $schoolId);
		$stmt->execute();
		$result = $stmt->get_result();
		
		$staffMembers = [];
		while ($row = $result->fetch_assoc()) {
			$staffMembers[$row['id']] = $row;
		}
		
		$this->logger->log("Found " . count($staffMembers) . " staff members for school ID: $schoolId");
		return $staffMembers;
	}

	public function processStaffMember($staffMember, $schoolId) {
		// Skip special case staff ID 1 (this was a special case in the old code)
		if (isset($staffMember['id']) && $staffMember['id'] == 1) {
			$this->logger->log("SKIPPING PROCESSING for special case staff ID 1", 'WARNING');
			return 1;
		}
		
		// Validate required fields
		if (empty($schoolId) || !is_numeric($schoolId) || $schoolId <= 0) {
			throw new RuntimeException("Invalid school ID for staff processing: " . var_export($schoolId, true));
		}
		
		if (empty($staffMember['full_name']) || strlen(trim($staffMember['full_name'])) < 2) {
			$this->logger->log("Rejecting staff with invalid name: " . var_export($staffMember, true), 'WARNING');
			throw new RuntimeException("Staff member must have a valid name (at least 2 characters)");
		}
		
		// Clean and sanitize the full name
		$staffMember['full_name'] = $this->sanitizeString(trim($staffMember['full_name']));
		
		$this->checkConnectionDeeper();
		
		try {
			// Look for staff member at THIS SPECIFIC SCHOOL with this name
			$stmt = $this->conn->prepare("
				SELECT s.id 
				FROM csd_staff s
				JOIN csd_school_staff ss ON s.id = ss.staff_id
				WHERE s.full_name = ? AND ss.school_id = ?
			");
			
			$stmt->bind_param("si", $staffMember['full_name'], $schoolId);
			$stmt->execute();
			$result = $stmt->get_result();
			
			if ($result->num_rows > 0) {
				// Update existing staff member AT THIS SCHOOL
				$row = $result->fetch_assoc();
				$staffId = $row['id'];
				$stmt->close();
				
				// Get current record for comparison
				$stmt = $this->conn->prepare("SELECT * FROM csd_staff WHERE id = ?");
				$stmt->bind_param("i", $staffId);
				$stmt->execute();
				$currentRecord = $stmt->get_result()->fetch_assoc();
				$stmt->close();
				
				// Only update if data has actually changed
				if ($this->hasDataChanged($currentRecord, $staffMember)) {
					$this->logger->log("Data changed for staff member {$staffMember['full_name']}, updating...");
					
					$stmt = $this->conn->prepare("
						UPDATE csd_staff 
						SET title = ?, phone = ?, email = ?, sport_department = ?, date_updated = NOW()
						WHERE id = ?
					");
					
					$title = $this->sanitizeString(trim($staffMember['title'] ?? ''));
					$phone = trim($staffMember['phone'] ?? '');
					$email = strtolower(trim($staffMember['email_address'] ?? ''));
					$sportDept = $this->sanitizeString(trim($staffMember['sport_department'] ?? ''));
					
					$stmt->bind_param("ssssi", $title, $phone, $email, $sportDept, $staffId);
					
					if (!$stmt->execute()) {
						throw new RuntimeException("Failed to update staff record: " . $stmt->error);
					}
					
					$stmt->close();
					
					// No need to modify relationships - this person is already at this school
				} else {
					$this->logger->log("No changes needed for staff member {$staffMember['full_name']}");
					// No updates or relationship changes needed
				}
			} else {
				// This is a NEW staff member for THIS school
				// Instead of checking for staff with same name elsewhere, we ALWAYS create a new record
				$stmt->close();
				$this->logger->log("Creating new staff record for " . $staffMember['full_name'] . " at school $schoolId");
				
				$stmt = $this->conn->prepare("
					INSERT INTO csd_staff (full_name, title, phone, email, sport_department, date_created, date_updated)
					VALUES (?, ?, ?, ?, ?, NOW(), NOW())
				");
				
				$title = $this->sanitizeString(trim($staffMember['title'] ?? ''));
				$phone = trim($staffMember['phone'] ?? '');
				$email = strtolower(trim($staffMember['email_address'] ?? ''));
				$sportDept = $this->sanitizeString(trim($staffMember['sport_department'] ?? ''));
				
				$stmt->bind_param("sssss", $staffMember['full_name'], $title, $phone, $email, $sportDept);
				
				if (!$stmt->execute()) {
					throw new RuntimeException("Failed to create staff record: " . $stmt->error);
				}
				
				$staffId = $this->conn->insert_id;
				$stmt->close();
				
				// Create relationship with school
				$stmt = $this->conn->prepare("
					INSERT INTO csd_school_staff (school_id, staff_id)
					VALUES (?, ?)
				");
				
				$stmt->bind_param("ii", $schoolId, $staffId);
				
				if (!$stmt->execute()) {
					// If relationship creation fails, delete the staff member
					$this->removeStaffMember($staffId);
					throw new RuntimeException("Failed to create school-staff relationship: " . $stmt->error);
				}
				
				$stmt->close();
			}
			
			return $staffId;
		} catch (Exception $e) {
			$this->logger->log("Error processing staff member {$staffMember['full_name']}: " . $e->getMessage(), 'ERROR');
			throw $e;
		}
	}
	
	private function hasDataChanged($current, $new) {
		$fields = [
			'title' => 'title',
			'phone' => 'phone',
			'email' => 'email_address',
			'sport_department' => 'sport_department'
		];
		
		foreach ($fields as $dbField => $newField) {
			$currentValue = trim($current[$dbField] ?? '');
			$newValue = trim($new[$newField] ?? '');
			
			if ($currentValue !== $newValue) {
				$this->logger->log("Field '$dbField' changed from '$currentValue' to '$newValue'");
				return true;
			}
		}
		
		return false;
	}

	public function preventMultipleSchoolRelationships($staffId) {
		$this->logger->log("PREVENTIVE CHECK: Ensuring staff ID $staffId has no multiple school relationships");
		
		// Get all schools for this staff member
		$stmt = $this->conn->prepare("
			SELECT school_id, id
			FROM csd_school_staff
			WHERE staff_id = ?
		");
		
		$stmt->bind_param("i", $staffId);
		$stmt->execute();
		$result = $stmt->get_result();
		
		$schools = [];
		while ($row = $result->fetch_assoc()) {
			$schools[] = $row;
		}
		$stmt->close();
		
		if (count($schools) > 1) {
			$this->logger->log("PREVENTING ISSUE: Found " . count($schools) . " existing school relationships for staff ID $staffId");
			
			// Keep only the most recent relationship
			usort($schools, function($a, $b) {
				return $b['id'] - $a['id']; // Higher ID is newer
			});
			
			$keepRelation = $schools[0];
			$this->logger->log("Keeping only the most recent relationship ID: {$keepRelation['id']} with school ID: {$keepRelation['school_id']}");
			
			// Remove all others
			for ($i = 1; $i < count($schools); $i++) {
				$this->logger->log("Removing excess relationship ID: {$schools[$i]['id']} with school ID: {$schools[$i]['school_id']}");
				
				$stmt = $this->conn->prepare("
					DELETE FROM csd_school_staff
					WHERE id = ?
				");
				
				$stmt->bind_param("i", $schools[$i]['id']);
				$stmt->execute();
				$stmt->close();
			}
			
			// Verify cleanup worked
			$stmt = $this->conn->prepare("
				SELECT COUNT(*) as count
				FROM csd_school_staff
				WHERE staff_id = ?
			");
			
			$stmt->bind_param("i", $staffId);
			$stmt->execute();
			$count = $stmt->get_result()->fetch_assoc()['count'];
			$stmt->close();
			
			if ($count > 1) {
				$this->logger->log("WARNING: Staff ID $staffId still has multiple schools after cleanup attempt!", 'WARNING');
			} else {
				$this->logger->log("Successfully fixed multiple relationships for staff ID $staffId");
			}
		}
	}

	public function removeSchoolStaffRelationship($schoolId, $staffId) {
		$this->logger->log("Removing relationship between staff $staffId and school $schoolId");
		
		$stmt = $this->conn->prepare("
			DELETE FROM csd_school_staff
			WHERE school_id = ? AND staff_id = ?
		");
		
		$stmt->bind_param("ii", $schoolId, $staffId);
		$result = $stmt->execute();
		$stmt->close();
		
		return $result;
	}

	public function removeStaffMember($staffId) {
		$this->logger->log("Removing staff member: $staffId");
		
		try {
			// First remove relationships
			$stmt = $this->conn->prepare("
				DELETE FROM csd_school_staff
				WHERE staff_id = ?
			");
			
			$stmt->bind_param("i", $staffId);
			$stmt->execute();
			$stmt->close();
			
			// Then remove staff record
			$stmt = $this->conn->prepare("
				DELETE FROM csd_staff
				WHERE id = ?
			");
			
			$stmt->bind_param("i", $staffId);
			$result = $stmt->execute();
			$stmt->close();
			
			return $result;
		} catch (Exception $e) {
			$this->logger->log("Error removing staff member: " . $e->getMessage(), 'ERROR');
			return false;
		}
	}
	
	// Add near the end of the DbClient class, before the class closing brace
	public function logConnectionStatus() {
		$this->checkConnection();
		$statusQuery = "SHOW STATUS WHERE Variable_name IN ('Threads_connected', 'Uptime', 'Queries')";
		$result = $this->conn->query($statusQuery);
		
		$status = [];
		while ($row = $result->fetch_assoc()) {
			$status[$row['Variable_name']] = $row['Value'];
		}
		
		$this->logger->log("MySQL Connection Status: " . json_encode($status), 'INFO');
	}
}

class WebhookQueue {
	private $logger;
	private $queueDir;
	
	public function __construct() {
		$this->logger = Logger::getInstance();
		$this->queueDir = QUEUE_DIR;
	}
	
	public function enqueue($data) {
		$filename = $this->queueDir . '/' . uniqid('webhook_', true) . '.json';
		
		if (file_put_contents($filename, json_encode($data)) === false) {
			throw new RuntimeException("Failed to write webhook data to queue");
		}
		
		$this->logger->log("Queued webhook data: " . basename($filename));
		return basename($filename);
	}
	
	public function processNext() {
		$this->cleanup();  // Clean up old queue items first
		
		$files = glob($this->queueDir . '/webhook_*.json');
		if (empty($files)) {
			return false;
		}
		
		usort($files, function($a, $b) {
			return filemtime($a) - filemtime($b);
		});
		
		$nextFile = $files[0];
		$lockFile = $nextFile . '.lock';
		
		$fp = fopen($lockFile, 'w');
		if (!flock($fp, LOCK_EX | LOCK_NB)) {
			fclose($fp);
			return false;
		}
		
		try {
			$fileContents = file_get_contents($nextFile);
			if ($fileContents === false) {
				throw new RuntimeException("Could not read queue file: " . $nextFile);
			}
			
			$data = json_decode($fileContents, true);
			if ($data === null) {
				throw new RuntimeException("Invalid JSON in queue file: " . $nextFile);
			}
			
			// Store file info with the data for cleanup after successful processing
			$data['_queue_file'] = $nextFile;
			$data['_lock_file'] = $lockFile;
			
			return $data;
		} catch (Exception $e) {
			$this->logger->log("Queue processing error: " . $e->getMessage(), 'ERROR');
			if (file_exists($lockFile)) {
				unlink($lockFile);
			}
			throw $e;
		} finally {
			flock($fp, LOCK_UN);
			fclose($fp);
		}
	}
	
	public function completeItem($data) {
		if (isset($data['_queue_file']) && file_exists($data['_queue_file'])) {
			unlink($data['_queue_file']);
		}
		if (isset($data['_lock_file']) && file_exists($data['_lock_file'])) {
			unlink($data['_lock_file']);
		}
	}
	
	public function cleanup() {
		$files = glob($this->queueDir . '/webhook_*.json');
		foreach ($files as $file) {
			if (time() - filemtime($file) > MAX_QUEUE_AGE) {
				//unlink($file);
				//$this->logger->log("Removed stale queue file: " . basename($file));
			}
		}
	}
	
	public function getQueueStats() {
		$files = glob($this->queueDir . '/webhook_*.json');
		return [
			'total_items' => count($files),
			'oldest_item' => !empty($files) ? filemtime($files[0]) : null,
			'queue_size' => array_sum(array_map('filesize', $files))
		];
	}
}

class WebhookHandler {
	private $logger;
	private $openAiClient;
	public $dbClient;
	private $queue;
	private $rateLimiter;
	private $lockFile;
	use StringSanitizerTrait;
	
	public function __construct() {
		$this->logger = Logger::getInstance();
		//$this->openAiClient = new OpenAiClient(OPENAI_API_KEY);
		// Use Enhanced OpenAI Client for parallel processing
		$this->openAiClient = new EnhancedOpenAiClient(OPENAI_API_KEY, $this->logger);
		$this->dbClient = new DbClient();
		$this->queue = new WebhookQueue();
		$this->rateLimiter = new RateLimiter();
		$this->lockFile = LOCK_DIR . '/webhook_processor.lock';
	}
	
	public function handleRequest() {
		try {
			// Only perform these checks for web requests
			if (php_sapi_name() !== 'cli') {
				$this->validateApiKey();
				$this->checkMemoryUsage();
				
				if (!$this->rateLimiter->checkLimit($_SERVER['REMOTE_ADDR'])) {
					$this->sendResponse(['status' => 'error', 'message' => 'Rate limit exceeded']);
				}
				
				$data = $this->parseWebhookData();
				$queueId = $this->queue->enqueue($data);
				
				$this->sendResponse([
					'status' => 'success',
					'message' => 'Webhook queued for processing',
					'queue_id' => $queueId
				]);
			}
		} catch (Exception $e) {
			$this->logger->log("Error handling webhook: " . $e->getMessage(), 'ERROR');
			$this->sendResponse([
				'status' => 'error',
				'message' => 'Internal server error',
				'error' => $e->getMessage()
			]);
		}
	}
	
	private function triggerBackgroundProcessing() {
		// Queue will be processed by cron job
		$this->logger->log("Queue item added - will be processed by cron job");
		return true;
	}
	
	private function validateApiKey() {
		$sapi = php_sapi_name();
		$this->logger->log("Validating API key. SAPI: " . $sapi);
		
		// Skip validation for CLI
		if ($sapi === 'cli') {
			$this->logger->log("Skipping API key validation for CLI execution");
			return true;
		}
		
		// For web requests, validate API key
		$headers = array_change_key_case(getallheaders(), CASE_LOWER);
		$apiKey = $headers['api-key'] ?? '';
		
		if (!hash_equals(API_KEY, $apiKey)) {
			$this->logger->log("Invalid API key attempt", 'WARNING');
			$this->sendResponse(['status' => 'error', 'message' => 'Unauthorized access']);
		}
		
		$this->logger->log("API key validated successfully");
	}

	private function parseWebhookData() {
		$input = file_get_contents("php://input");
		if (empty($input)) {
			throw new RuntimeException("No input data received");
		}
		
		$data = json_decode($input, true);
		if (json_last_error() !== JSON_ERROR_NONE) {
			throw new RuntimeException("Invalid JSON received: " . json_last_error_msg());
		}
		
		return $data;
	}

	private function checkMemoryUsage() {
		$limit = $this->returnBytes(ini_get('memory_limit'));
		$current = memory_get_usage(true);
		$peak = memory_get_peak_usage(true);
		
		$this->logger->log("Memory usage: Current={$current}, Peak={$peak}, Limit={$limit}");
		
		if ($current > $limit * 0.9) {
			throw new RuntimeException("Memory usage exceeded safe threshold");
		}
	}

	private function returnBytes($val) {
		$val = trim($val);
		$last = strtolower($val[strlen($val)-1]);
		$val = (int)$val;
		switch($last) {
			case 'g': $val *= 1024;
			case 'm': $val *= 1024;
			case 'k': $val *= 1024;
		}
		return $val;
	}

	private function sendResponse($response) {
		header('Content-Type: application/json');
		echo json_encode($response);
		exit;
	}

	public function processQueueItem() {
		// Try to get process lock
		$fp = fopen($this->lockFile, 'w+');
		if (!flock($fp, LOCK_EX | LOCK_NB)) {
			$this->logger->log("Another process is already running", 'INFO');
			fclose($fp);
			return false;
		}

		try {
			$startTime = time();
			$data = $this->queue->processNext();
			if ($data === false) {
				$this->logger->log("No items in queue to process", 'INFO');
				return false;
			}
			
			if ($data['action'] === 'changedetection_notify') {
				$this->logger->log("Processing queued webhook - UUID: " . $data['UUID']);
				
				// Check if max runtime exceeded
				if (time() - $startTime > QUEUE_PROCESSOR_MAX_RUNTIME) {
					$this->logger->log("Maximum runtime exceeded, terminating", 'WARNING');
					return false;
				}
				
				$this->processContentChange($data);
				// Only remove queue file after successful processing
				$this->queue->completeItem($data);
				return true;
			}
		} catch (Exception $e) {
			$this->logger->log("Error processing queue item: " . $e->getMessage(), 'ERROR');
			return false;
		} finally {
			// Always release the lock and cleanup
			flock($fp, LOCK_UN);
			fclose($fp);
			@unlink($this->lockFile);
		}
	}

	private function processContentChange($data) {
		$startMemory = memory_get_usage(true);
		$this->logger->log("Starting memory usage: " . $this->formatBytes($startMemory));
		
		// Fix for ID 1 issue - run before every content change
		$this->logger->log("Running preventive cleanup for known problematic staff IDs");
		
		// Clean up staff ID 1 which seems particularly problematic
		try {
			$this->dbClient->preventMultipleSchoolRelationships(1);
		} catch (Exception $e) {
			$this->logger->log("Error cleaning up staff ID 1: " . $e->getMessage(), 'WARNING');
		}
		
		try {
			// Initialize API processor
			$apiProcessor = new ApiDataProcessor();
			
			$this->logger->log("=== Starting Content Type Detection ===");
			$this->logger->log("Content snapshot length: " . strlen($data['current_snapshot']));
			
			// Detect the type of content
			$apiType = $apiProcessor->detectApiType($data['current_snapshot']);
			$this->logger->log("Detection result: " . $apiType);
			
			// Process the content based on type
			if ($apiType === 'unstructured') {
				$this->logger->log("=== Processing as Unstructured Content ===");
				$staffResponseRaw = $this->openAiClient->getStructuredStaffData($data['current_snapshot']);
				
				// Validate OpenAI response
				if (!$this->validateOpenAiResponse($staffResponseRaw)) {
					throw new RuntimeException("Invalid staff data received from OpenAI API");
				}
				
				$staffResponse = json_decode($staffResponseRaw, true);
				$this->logger->log("OpenAI processing complete");
			} else {
				// Similar validation for structured API responses
				$this->logger->log("=== Processing as Structured " . strtoupper($apiType) . " Data ===");
				$staffResponse = $apiProcessor->processApiData($data['current_snapshot'], $apiType);
				
				if (empty($staffResponse) || !isset($staffResponse['staff_members'])) {
					$this->logger->log("Invalid response from API processor", 'ERROR');
					throw new RuntimeException("Invalid or empty staff data from API processor");
				}
				
				$this->logger->log("API processing complete");
			}
			
			// Force a brand new DB connection after the lengthy OpenAI operation
			$this->logger->log("DEBUG: Forcing new database connection after OpenAI processing");
			$this->dbClient = new DbClient(); // Create a completely new client with fresh connection
			$this->logger->log("DEBUG: Fresh database connection established");
			
			$currentMemory = memory_get_usage(true);
			$this->logger->log("Memory usage after API processing: " . $this->formatBytes($currentMemory));
			$this->logger->log("Memory increase: " . $this->formatBytes($currentMemory - $startMemory));
			
			$this->logger->logArray($staffResponse);
			
			$staffData = $staffResponse['staff_members'] ?? $staffResponse;
			
			if (!is_array($staffData)) {
				throw new RuntimeException("Invalid staff data received");
			}
			
			$this->logger->log("Aggregating staff data");
			$aggregatedStaff = $this->aggregateStaffData($staffData);
			$this->logger->log("Staff data aggregated. Total staff members: " . count($aggregatedStaff));
			
			// Add this right after aggregating staff data
			$this->logger->log("DEBUG: About to call getSchoolInfo for: " . $data['school_name']);
			
			// Try printing the entire data array to see if school_name exists
			$this->logger->log("DEBUG: Full data array contents: " . print_r($data, true));
			
			try {
				$schoolResponse = $this->openAiClient->getSchoolInfo($data['school_name']);
				$this->logger->log("DEBUG: Successfully got school info response");
			} catch (Exception $e) {
				$this->logger->log("CRITICAL ERROR: getSchoolInfo failed: " . $e->getMessage() . "\n" . $e->getTraceAsString(), 'ERROR');
				// Rethrow to ensure it's caught by the main try/catch
				throw $e;
			}
						
			// Add heartbeat here:
			// $this->dbClient->keepConnectionAlive();
			
			// Temporarily bypass database heartbeat
			try {
				$this->logger->log("DEBUG: About to perform database heartbeat");
				$this->dbClient->keepConnectionAlive();
				$this->logger->log("DEBUG: Database heartbeat successful");
			} catch (Throwable $e) {
				$this->logger->log("CRITICAL: Database heartbeat failed: " . $e->getMessage() . "\n" . $e->getTraceAsString(), 'ERROR');
				// Continue anyway for testing
				$this->logger->log("DEBUG: Continuing after database heartbeat failure");
			}
			
			// Add right after staff data aggregation
			$this->logger->log("DEBUG: Testing database connection before school processing");
			try {
				// Force a simple query to test connection
				$this->dbClient->keepConnectionAlive();
				$this->logger->log("DEBUG: Database connection is alive");
			} catch (Exception $e) {
				$this->logger->log("CRITICAL ERROR: Database connection lost: " . $e->getMessage(), 'ERROR');
				throw $e;
			}
			
			// Add right after aggregating staff data
			$this->logger->log("DEBUG: Memory usage before school processing: " . 
				$this->formatBytes(memory_get_usage(true)) . 
				", Peak: " . $this->formatBytes(memory_get_peak_usage(true)));
			
			$actualSchoolName = $data['school_name'];
			$lookupSchoolName = $data['school_name'];
			$this->logger->log("Getting school info for: " . $data['school_name']);
			
			$schoolResponse = $this->openAiClient->getSchoolInfo($data['school_name']);
			$this->logger->log("Received school info from OpenAI");
			$this->logger->logArray(json_decode($schoolResponse, true));
			
			$this->logger->log("DEBUG: About to parse school info response");
			
			try {
				$schoolInfo = $this->parseSchoolInfo($schoolResponse);
				$this->logger->log("DEBUG: Successfully parsed school info");
			} catch (Exception $e) {
				$this->logger->log("CRITICAL ERROR: Failed to parse school info: " . $e->getMessage(), 'ERROR');
				throw $e;
			}
			
			// Also check if there's a die() or exit() in the parseSchoolInfo method
			$this->logger->log("DEBUG: School name after parsing: " . $schoolInfo['school_name']);
			
			// Keep the school name consistent with what was used for lookup
			$schoolInfo['school_name'] = $lookupSchoolName;
			
			try {
				$this->logger->log("Processing school in database: " . $schoolInfo['school_name']);
				$schoolId = $this->dbClient->processSchool($schoolInfo);
				$this->logger->log("School processed. School ID: " . $schoolId);
			} catch (Exception $e) {
				$this->logger->log("Error processing school: " . $e->getMessage(), 'ERROR');
				// Try to create a new connection and retry once
				$this->dbClient = new DbClient();
				$this->logger->log("Created new connection, retrying school processing");
				$schoolId = $this->dbClient->processSchool($schoolInfo);
			}
			
			$this->logger->log("Fetching current staff members for school");
			$currentStaffMembers = $this->dbClient->getStaffMembersForSchool($schoolId);
			$this->logger->log("Current staff members retrieved. Count: " . count($currentStaffMembers));
			
			$this->logger->log("Starting staff member updates");
			$metrics = $this->updateStaffMembers($currentStaffMembers, $aggregatedStaff, $schoolId);
			$this->logger->log("Staff member updates completed");
			
			$this->logger->log("SUCCESS METRICS: " . 
			"Staff processed: {$metrics['processed']}, " .
			"Staff updated: {$metrics['updated']}, " .
			"Staff created: {$metrics['created']}, " .
			"Staff removed: {$metrics['removed']}", 'INFO');
			
			$finalMemory = memory_get_usage(true);
			$this->logger->log("Final memory usage: " . $this->formatBytes($finalMemory));
			$this->logger->log("Total memory increase: " . $this->formatBytes($finalMemory - $startMemory));
			
		} catch (Exception $e) {
			// Add trace information for easier debugging
			$this->logger->log("CRITICAL ERROR in processContentChange: " . $e->getMessage() . 
				"\nTrace: " . $e->getTraceAsString() . 
				"\nPrevious exception: " . ($e->getPrevious() ? $e->getPrevious()->getMessage() : 'none'), 'ERROR');
			$this->logger->log("Final error memory usage: " . $this->formatBytes(memory_get_usage(true)));
			throw $e;
		}
		
		// At the end of processContentChange, add verification:
		$this->logger->log("Running final verification checks...");
		$verificationStaffMembers = $this->dbClient->getStaffMembersForSchool($schoolId);
		$this->logger->log("Verification check: Found " . count($verificationStaffMembers) . 
			" staff members for school after processing");
	}
	
	private function formatBytes($bytes) {
		$units = ['B', 'KB', 'MB', 'GB'];
		$bytes = max($bytes, 0);
		$pow = floor(($bytes ? log($bytes) : 0) / log(1024));
		$pow = min($pow, count($units) - 1);
		$bytes /= pow(1024, $pow);
		return round($bytes, 2) . ' ' . $units[$pow];
	}
	
	private function updateStaffMembers($currentStaff, $newStaff, $schoolId) {
		if (empty($schoolId) || !is_numeric($schoolId) || $schoolId <= 0) {
			throw new RuntimeException("Invalid school ID for staff updates: " . var_export($schoolId, true));
		}
		
		// ADDED: Debug logging for array structure
		$this->logger->log("DEBUG: Starting updateStaffMembers function, memory usage: " . 
			$this->formatBytes(memory_get_usage(true)));
		$this->logger->log("DEBUG: Number of new staff to process: " . count($newStaff));
		
		// ADDED: Check newStaff array structure
		$firstKey = array_key_first($newStaff);
		$this->logger->log("DEBUG: First key type: " . gettype($firstKey) . ", value: " . var_export($firstKey, true));
		$firstValue = $newStaff[$firstKey];
		$this->logger->log("DEBUG: First value type: " . gettype($firstValue) . ", structure: " . var_export($firstValue, true));
		
		// Initialize metrics counters
		$metrics = [
			'processed' => count($newStaff),
			'updated' => 0,
			'created' => 0,
			'removed' => 0
		];
		
		$currentNames = array_map(function($staff) {
			return $staff['full_name'];
		}, $currentStaff);
		
		$newNames = array_keys($newStaff);
		
		$this->logger->log("Current staff members: " . implode(', ', $currentNames));
		$this->logger->log("New staff members: " . implode(', ', $newNames));
		
		$this->logger->log("DEBUG: Starting staff updates without transactions");
		
		// Skip transactions entirely for now
		
		// Remove staff no longer present
		$staffToRemove = [];
		foreach ($currentStaff as $staffId => $staffMember) {
			if (!in_array($staffMember['full_name'], $newNames)) {
				$staffToRemove[] = $staffId;
			}
		}
		
		// Process removals
		if (!empty($staffToRemove)) {
			$this->logger->log("Processing " . count($staffToRemove) . " staff removals");
			foreach ($staffToRemove as $staffId) {
				try {
					$staffMember = $currentStaff[$staffId];
					$this->logger->log("Removing: " . $staffMember['full_name']);
					$this->dbClient->removeSchoolStaffRelationship($schoolId, $staffId);
					$this->dbClient->removeStaffMember($staffId);
					$metrics['removed']++;
				} catch (Throwable $e) {
					$this->logger->log("Error removing staff: " . $e->getMessage(), 'ERROR');
				}
			}
		}
		
		// Process new/existing staff
		$this->logger->log("Processing " . count($newStaff) . " staff additions/updates");
		$processedCount = 0;
		
		foreach ($newStaff as $staffName => $staffMember) {
			try {
				$processedCount++;
				// Process in smaller batches
				if ($processedCount % 5 == 0) {
					$this->logger->log("Progress: $processedCount/" . count($newStaff));
					$this->dbClient->keepConnectionAlive();
				}
				
				// Validate staff member structure
				if (!isset($staffMember['full_name']) || empty($staffMember['full_name'])) {
					$this->logger->log("WARNING: Invalid staff structure, missing full_name", 'WARNING');
					$this->logger->logArray($staffMember);
					
					// If $staffName is the actual name, create a proper structure
					if (is_string($staffName) && !empty($staffName)) {
						$staffMember = [
							'full_name' => $staffName,
							'title' => $staffMember['title'] ?? '',
							'phone' => $staffMember['phone'] ?? '',
							'email_address' => $staffMember['email_address'] ?? '',
							'sport_department' => $staffMember['sport_department'] ?? ''
						];
						$this->logger->log("FIXED: Restructured staff data using key as name");
					} else {
						$this->logger->log("ERROR: Cannot fix staff data, skipping", 'ERROR');
						continue;
					}
				}
				
				$exists = false;
				// Check if this staff already exists
				foreach ($currentStaff as $existingId => $existingStaff) {
					if ($existingStaff['full_name'] === $staffMember['full_name']) {
						$exists = true;
						break;
					}
				}
				
				$staffId = $this->dbClient->processStaffMember($staffMember, $schoolId);
				
				// Track whether this was an update or a new record
				if ($exists) {
					$metrics['updated']++;
				} else {
					$metrics['created']++;
				}
				
				// Every 10 processed, force a heartbeat
				if ($processedCount % 10 == 0) {
					$this->dbClient->keepConnectionAlive();
				}
				
			} catch (Throwable $e) {
				$this->logger->log("Error processing staff member: " . $e->getMessage() . "\nTrace: " . $e->getTraceAsString(), 'ERROR');
			}
		}
		
		$this->logger->log("Staff member updates completed - " . 
			"Updated: {$metrics['updated']}, " . 
			"Created: {$metrics['created']}, " . 
			"Removed: {$metrics['removed']}");
		
		return $metrics;
	}
	
	private function aggregateStaffData($staffData) {
		$this->logger->log("Starting staff data aggregation");
		$this->logger->log("Initial staff count: " . count($staffData));
		
		$aggregated = [];
		$skippedCount = 0;
		$invalidNames = [];
		
		foreach ($staffData as $staffMember) {
			// ENHANCED VALIDATION - Skip staff with missing or invalid names
			if (empty($staffMember['full_name']) || strlen(trim($staffMember['full_name'])) < 2) {
				$invalidData = json_encode(array_slice($staffMember, 0, 3)); // Log part of the invalid data
				$this->logger->log("Skipping staff with invalid name: " . $invalidData, 'WARNING');
				$skippedCount++;
				$invalidNames[] = substr(var_export($staffMember['full_name'] ?? 'EMPTY', true), 0, 30);
				continue;
			}
			
			$fullName = $this->sanitizeString($staffMember['full_name']);
			
			// Initialize staff member if not already present
			if (!isset($aggregated[$fullName])) {
				$aggregated[$fullName] = [
					'full_name' => $this->sanitizeString($fullName),
					'title' => $this->sanitizeString($staffMember['title'] ?? ''),
					'phone' => trim($staffMember['phone'] ?? ''),
					'email_address' => strtolower(trim($staffMember['email_address'] ?? '')),
					'sport_department' => $this->sanitizeString($staffMember['sport_department'] ?? '')
				];
				$this->logger->log("Added new staff member: " . $fullName);
			} else {
				$this->logger->log("Merging additional data for: " . $fullName);
				
				// Only append non-empty values to title
				if (!empty($staffMember['title'])) {
					$existingTitle = $aggregated[$fullName]['title'] ?? '';
					$aggregated[$fullName]['title'] = $this->sanitizeString(
						$this->sanitizeString($existingTitle) . 
						(!empty($existingTitle) ? ', ' : '') . 
						$this->sanitizeString($staffMember['title'])
					);
				}
				
				// Only append non-empty values to sport_department
				if (!empty($staffMember['sport_department'])) {
					$existingDept = $aggregated[$fullName]['sport_department'] ?? '';
					$aggregated[$fullName]['sport_department'] = $this->sanitizeString(
						$existingDept . (!empty($existingDept) ? ', ' : '') . $staffMember['sport_department']
					);
				}
				
				// ENHANCED DATA MERGING - Prefer populated fields over empty ones
				if (!empty($staffMember['phone']) && empty($aggregated[$fullName]['phone'])) {
					$aggregated[$fullName]['phone'] = trim($staffMember['phone']);
				}
				
				if (!empty($staffMember['email_address']) && empty($aggregated[$fullName]['email_address'])) {
					$aggregated[$fullName]['email_address'] = strtolower(trim($staffMember['email_address']));
				}
			}
		}
		
		// Deduplicate fields for each staff member
		foreach ($aggregated as &$staffMember) {
			$this->logger->log("Deduplicating data for: " . $staffMember['full_name']);
			
			// Process title and sport_department if they exist and aren't empty
			if (!empty($staffMember['title'])) {
				$titles = array_map([$this, 'sanitizeString'], explode(',', $staffMember['title']));
				$titles = array_unique(array_filter($titles));
				$staffMember['title'] = implode(', ', $titles);
			}
			
			if (!empty($staffMember['sport_department'])) {
				$departments = array_map([$this, 'sanitizeString'], explode(',', $staffMember['sport_department']));
				$departments = array_unique(array_filter($departments));
				$staffMember['sport_department'] = implode(', ', $departments);
			}
			
			// ENHANCED VALIDATION - Ensure name is still valid after processing
			if (empty($staffMember['full_name']) || strlen($staffMember['full_name']) < 2) {
				$this->logger->log("Warning: Staff name became invalid after processing: " . 
					json_encode($staffMember), 'WARNING');
				// Fix the name to prevent blank records - use a placeholder if necessary
				$staffMember['full_name'] = $staffMember['full_name'] ?: 'Unknown Staff Member';
			}
		}
		
		// Log statistics about skipped records
		if ($skippedCount > 0) {
			$this->logger->log("Skipped $skippedCount invalid staff members during aggregation", 'WARNING');
			if (!empty($invalidNames)) {
				$this->logger->log("Invalid names skipped: " . implode(', ', array_slice($invalidNames, 0, 5)) . 
					(count($invalidNames) > 5 ? ' and ' . (count($invalidNames) - 5) . ' more' : ''), 'WARNING');
			}
		}
		
		// ENHANCED VALIDATION - Ensure we have some valid staff data
		if (empty($aggregated)) {
			$this->logger->log("WARNING: No valid staff members found after aggregation!", 'ERROR');
		}
		
		$this->logger->log("Staff aggregation complete. Final count: " . count($aggregated));
		return $aggregated;
	}
	
	/**
	 * PATCH: Add this additional method to WebhookHandler to enhance API response validation
	 */
	private function validateOpenAiResponse($response) {
		// Check if response is valid JSON
		if (empty($response)) {
			$this->logger->log("Empty response from API", 'ERROR');
			return false;
		}
		
		$decoded = json_decode($response, true);
		if (json_last_error() !== JSON_ERROR_NONE) {
			$this->logger->log("Invalid JSON in API response: " . json_last_error_msg(), 'ERROR');
			$this->logger->log("First 200 chars of response: " . substr($response, 0, 200), 'ERROR');
			return false;
		}
		
		// Check if response has the expected staff_members array
		if (!isset($decoded['staff_members']) || !is_array($decoded['staff_members'])) {
			$this->logger->log("API response missing staff_members array: " . json_encode(array_keys($decoded)), 'ERROR');
			return false;
		}
		
		// Check if staff_members array has any valid entries
		$validCount = 0;
		foreach ($decoded['staff_members'] as $staff) {
			if (!empty($staff['full_name']) && strlen(trim($staff['full_name'])) >= 2) {
				$validCount++;
			}
		}
		
		if ($validCount === 0) {
			$this->logger->log("No valid staff members found in API response", 'ERROR');
			return false;
		}
		
		$this->logger->log("API response validation passed: Found $validCount valid staff members");
		return true;
	}

	private function parseSchoolInfo($response) {
		$this->logger->log("Parsing school info response: " . $response);
		
		if (empty($response)) {
			throw new RuntimeException("Empty school info response received");
		}
		
		// More defensive JSON extraction
		try {
			if (strpos($response, '{') !== false) {
				$response = substr($response, strpos($response, '{'));
				$response = substr($response, 0, strrpos($response, '}') + 1);
			}
			
			$schoolInfo = json_decode($response, true);
			if ($schoolInfo === null) {
				throw new RuntimeException("Failed to parse school info: " . json_last_error_msg());
			}
		
			if (!isset($schoolInfo['school_colors']) || !is_array($schoolInfo['school_colors'])) {
				$schoolInfo['school_colors'] = [];
			}
			if (!isset($schoolInfo['school_divisions']) || !is_array($schoolInfo['school_divisions'])) {
				$schoolInfo['school_divisions'] = [];
			}
			if (!isset($schoolInfo['school_conferences']) || !is_array($schoolInfo['school_conferences'])) {
				$schoolInfo['school_conferences'] = [];
			}
			if (!isset($schoolInfo['school_address']) || !is_array($schoolInfo['school_address'])) {
				$schoolInfo['school_address'] = [
					'street_1' => '', 'street_2' => '', 'street_3' => '',
					'city' => '', 'county' => '', 'state' => '', 'zipcode' => '', 'country' => ''
				];
			}
			if (!isset($schoolInfo['school_level'])) {
				$schoolInfo['school_level'] = '';
			}
			if (!isset($schoolInfo['school_website'])) {
				$schoolInfo['school_website'] = '';
			}
			if (!isset($schoolInfo['athletics_website'])) {
				$schoolInfo['athletics_website'] = '';
			}
			if (!isset($schoolInfo['athletics_phone'])) {
				$schoolInfo['athletics_phone'] = '';
			}
			if (!isset($schoolInfo['mascot'])) {
				$schoolInfo['mascot'] = '';
			}
			if (!isset($schoolInfo['school_type'])) {
				$schoolInfo['school_type'] = '';
			}
			if (!isset($schoolInfo['school_enrollment'])) {
				$schoolInfo['school_enrollment'] = '';
			}
			if (!isset($schoolInfo['football_division']) || 
				!in_array($schoolInfo['football_division'], ['FBS', 'FCS', 'Neither'])) {
				$schoolInfo['football_division'] = 'Neither';
			}
			
			return $schoolInfo;
			
		} catch (Exception $e) {
			$this->logger->log("Error during JSON parsing: " . $e->getMessage() . "\nResponse: " . $response, 'ERROR');
			throw $e;
		}
	}
}

class ApiDataProcessor {
	private $logger;
	use StringSanitizerTrait;

	public function __construct() {
		$this->logger = Logger::getInstance();
	}

	public function detectApiType($content) {
		try {
			$this->logger->log("=== Starting API Type Detection ===");
			$this->logger->log("Content length: " . strlen($content));
			
			$data = json_decode($content, true);
			if (!$data) {
				$this->logger->log("JSON decode failed: " . json_last_error_msg());
				return 'unstructured';
			}
	
			$this->logger->log("Successfully parsed JSON. Checking structure...");
			
			// Since the data is an array of objects, check first element for structure
			if (is_array($data) && !empty($data[0])) {
				$firstItem = $data[0];
				
				// Check for groupName and groupItems structure
				if (isset($firstItem['groupName']) && isset($firstItem['groupItems'])) {
					$this->logger->log("✓ Detected groupItems API structure");
					return 'groupitems';
				}
				$this->logger->log("✗ Not groupItems format");
	
				// Check for department_staff_member_positions format
				if (isset($firstItem['department_staff_member_positions'])) {
					$this->logger->log("✓ Detected department staff members API structure");
					return 'dept_staff';
				}
				$this->logger->log("✗ Not department staff format");
			}
	
			// Check for Sidearm Sports format
			if (isset($data['groups']) && is_array($data['groups'])) {
				$this->logger->log("✓ Detected Sidearm Sports API structure");
				return 'sidearm';
			}
			$this->logger->log("✗ Not Sidearm format");
	
			// Check for WMT Digital format
			if (isset($data['data']) && isset($data['data'][0]['department_staff_member_positions'])) {
				$this->logger->log("✓ Detected WMT Digital API structure");
				return 'wmt';
			}
			$this->logger->log("✗ Not WMT format");
	
			$this->logger->log("No known API format detected, treating as unstructured");
			return 'unstructured';
			
		} catch (Exception $e) {
			$this->logger->log("Error in detectApiType: " . $e->getMessage(), 'ERROR');
			return 'unstructured';
		}
	}

	public function processApiData($content, $apiType) {
		switch ($apiType) {
			case 'sidearm':
				return $this->processSidearmData($content);
			case 'wmt':
				return $this->processWmtData($content);
			case 'groupitems':
				return $this->processGroupItemsData($content);
			case 'dept_staff':
				return $this->processDepartmentStaffData($content);
				break;
			default:
				throw new RuntimeException("Unsupported API type: " . $apiType);
		}
	}
	
	private function processDepartmentStaffData($content) {
		$data = json_decode($content, true);
		$staffMembers = [];
	
		// Process each department
		foreach ($data as $department) {
			if (!isset($department['department_staff_member_positions']) || 
				!is_array($department['department_staff_member_positions'])) {
				continue;
			}
	
			$departmentName = $department['name'] ?? '';
			
			foreach ($department['department_staff_member_positions'] as $position) {
				if (empty($position) || !isset($position['staff_member'])) {
					continue;
				}
	
				$member = $position['staff_member'];
				
				$fullName = $fullName = $this->sanitizeString(trim(($member['first_name'] ?? '') . ' ' . ($member['last_name'] ?? '')));
				if (empty($fullName)) {
					continue;
				}
	
				$staffMembers[] = [
					'full_name' => $this->sanitizeString($fullName),
					'title' => $this->sanitizeString($position['position'] ?? $member['position'] ?? ''),
					'email_address' => strtolower(trim($member['email'] ?? '')),
					'phone' => trim($member['phone'] ?? ''),
					'sport_department' => $this->sanitizeString($departmentName)
				];
			}
		}
	
		$this->logger->log("Processed " . count($staffMembers) . " staff members from department staff format");
		return ['staff_members' => $staffMembers];
	}
	
	private function processGroupItemsData($content) {
		$data = json_decode($content, true);
		$staffMembers = [];
	
		// Process each group in the array
		foreach ($data as $group) {
			if (!isset($group['groupItems']) || !isset($group['groupName'])) {
				continue;
			}
	
			$departmentName = $group['groupName'];
			
			foreach ($group['groupItems'] as $item) {
				// Skip empty items or items without basic required info
				if (empty($item) || !isset($item['firstName']) || !isset($item['lastName'])) {
					continue;
				}
	
				// Construct full name from firstName and lastName
				$fullName = $this->sanitizeString(trim($item['firstName'] . ' ' . $item['lastName']));
				if (empty($fullName)) {
					continue;
				}
	
				$staffMembers[] = [
					'full_name' => $this->sanitizeString($fullName),
					'title' => $this->sanitizeString($item['title'] ?? ''),
					'email_address' => strtolower(trim($item['email'] ?? '')),
					'phone' => trim($item['phone'] ?? ''),
					'sport_department' => $this->sanitizeString($departmentName)
				];
			}
		}
	
		$this->logger->log("Processed " . count($staffMembers) . " staff members from groupItems format");
		
		// Log the first staff member as a sample for verification
		if (!empty($staffMembers)) {
			$this->logger->log("Sample staff member:");
			$this->logger->logArray($staffMembers[0]);
		}
	
		return ['staff_members' => $staffMembers];
	}

	private function processSidearmData($content) {
		$data = json_decode($content, true);
		$staffMembers = [];

		foreach ($data['groups'] as $group) {
			$sportDepartment = $group['name'];
			
			foreach ($group['members'] as $member) {
				$staffMembers[] = [
					'full_name' => $this->sanitizeString(trim($member['firstName']) . ' ' . trim($member['lastName'])),
					'title' => $this->sanitizeString($member['title'] ?? ''),
					'email_address' => strtolower(trim($member['email'] ?? '')),
					'phone' => trim($member['phone'] ?? ''),
					'sport_department' => $this->sanitizeString($sportDepartment)
				];
			}
		}

		return ['staff_members' => $staffMembers];
	}

	private function processWmtData($content) {
		$data = json_decode($content, true);
		$staffMembers = [];

		foreach ($data['data'] as $department) {
			$sportDepartment = $department['name'];
			
			foreach ($department['department_staff_member_positions'] as $position) {
				$member = $position['staff_member'];
				
				// Extract phone and email from profile field values
				$phone = '';
				$email = '';
				foreach ($member['profile_field_values'] as $field) {
					if ($field['profile_field']['name'] === 'Phone') {
						$phone = $field['value'];
					} elseif ($field['profile_field']['name'] === 'Email') {
						$email = $field['value'];
					}
				}

				$staffMembers[] = [
					'full_name' => $this->sanitizeString(trim($member['first_name']) . ' ' . trim($member['last_name'])),
					'title' => $this->sanitizeString($position['title'] ?? ''),
					'email_address' => strtolower(trim($email)),
					'phone' => trim($phone),
					'sport_department' => $this->sanitizeString($sportDepartment)
				];
			}
		}

		return ['staff_members' => $staffMembers];
	}
}

// Initialize and validate configuration
try {
	ConfigValidator::validate();
		
	// Add this temporary cache clearing code here
	array_map('unlink', glob(sys_get_temp_dir() . '/webhook_cache/*'));
	
} catch (Exception $e) {
	die("Configuration error: " . $e->getMessage());
}

// Initialize diagnostic logging
$logger = Logger::getInstance();
$logger->log("=== Script Started ===");

// Initialize and run the webhook handler
try {
	$handler = new WebhookHandler();
	$sapi = php_sapi_name();
	$logger->log("Executing in mode: " . $sapi);
	
	if ($sapi === 'cli') {
		$logger->log("Running in CLI mode - processing queue");
		$handler->processQueueItem();
	} else {
		$logger->log("Running in web mode - handling request");
		$handler->handleRequest();
	}
} catch (Exception $e) {
	$logger->log("Fatal error: " . $e->getMessage(), 'ERROR');
	if (php_sapi_name() !== 'cli') {
		header('HTTP/1.1 500 Internal Server Error');
		echo json_encode([
			'status' => 'error',
			'message' => 'Internal server error',
			'error' => $e->getMessage()
		]);
	}
} finally {
	$logger->log("=== Script Completed ===");
	die;
}
?>
