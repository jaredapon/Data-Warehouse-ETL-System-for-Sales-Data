<?php
// Ensure message.log exists
$messageLog = __DIR__ . '/message.log';
if (!file_exists($messageLog)) {
    file_put_contents($messageLog, '');
}

// Read the raw POST data from the request body
$data = json_decode(file_get_contents('php://input'), true);

// Check if the 'message' key exists in the decoded data
if (isset($data['message'])) {
    // Append the message to 'message.log' file
    error_log($data['message'] . "\n", 3, $messageLog);
}
?>