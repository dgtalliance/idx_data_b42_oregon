<?php

use PhpAmqpLib\Message\AMQPMessage;

require_once '../autoload.php';

$helpers = new Helpers();
$board = GlobalVariables::BOARD_ID;

$RabbitCon = new RabbitConection($board);

/**
 * @param $message AMQPMessage
 */
$callback = function (AMQPMessage $message) use ($RabbitCon, $helpers) {
    $payload = json_decode($message->body, true);
    $commands = [];
    foreach ($payload as $property) {
        IdxLogger::setLog("Property wit ListingId: {$property['ListingId']}  will be updated");
        $commands[] = "php allProcess.php --mls {$property['ListingId']} --sysId {$property['ListingKey']} --sourceId {$property['SourceId']} --status {$property['Status']} > photo.log 2>&1 &";
    }

    foreach ($commands as $command) {
        // wait for 30 milliseconds
        usleep(200000);
        shell_exec($command);
    }
    return $message->nack();
};


$RabbitCon->channel->basic_qos(null, 1, null);

$RabbitCon->channel->basic_consume(
    $RabbitCon->queue_name_photos,
    '',
    false,
    false,
    false,
    false,
    $callback
);
while ($RabbitCon->channel->is_consuming()) {
    try {
        $RabbitCon->channel->wait();
    } catch (\ErrorException $e) {
        var_dump($e->getMessage());
        die;
    }
}

// Close channel connection
$RabbitCon->channel->close();

// Close broker connection
$RabbitCon->broker->close();
