<?php

use PhpAmqpLib\Message\AMQPMessage;

$initTime = date("H:i:s");
require_once '../autoload.php';
$helpers = new Helpers();

$board = GlobalVariables::BOARD_ID;

$status = getopt('', ["status:"]);
$status = (isset($status['status']) ? $status['status'] : 'active');


$status = ucfirst($status);
$RabbitCon = new RabbitConection($board);


$message = new AMQPMessage('', [
    'content_type' => 'text/plain',
    'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT
]);

$result = $helpers->getPropertiesForUpdatePhotos($board, $status);

$properties = array_chunk($result, 10);
foreach ($properties as $property) {
    $messagesQueve = [];
    foreach ($property as $value) {
        $value['Status'] = $status;
        $value['SourceId'] = $board;
        $messagesQueve[] = $value;

    }
    $message->setBody(json_encode($messagesQueve));
    $RabbitCon->channel->basic_publish(
        $message,
        '',
        $RabbitCon->queue_name_photos
    );
    $mls_list = "'" . implode("','", array_column($property, 'ListingId')) . "'";
    IdxLogger::setLog('Published message with MLSNumbers: ' . implode(",", array_column($property, 'ListingId')), IdxLog::type_confirmation);
    $helpers->updateMediaStatus($mls_list, $status, 2);
}

// Close channel connection
$RabbitCon->channel->close();

// Close broker connection
$RabbitCon->broker->close();
$helpers->logTimer($initTime);