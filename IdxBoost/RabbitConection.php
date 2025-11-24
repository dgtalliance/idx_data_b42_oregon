<?php

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;

class RabbitConection
{
    /** @var AMQPStreamConnection */
    public $broker;

    public $queue_name_photos;
    /** @var AMQPChannel */
    public $channel;

    public $enqueued_messages = 0;

    public function __construct($board)
    {

        $this->broker = new AMQPStreamConnection(
            'bobbish-black-panther.rmq.cloudamqp.com',
            5672,
            'jitctbjj',
            'kkyEIQmUNcNwVvVP_3yziUIosh8Nl3OW',
            'jitctbjj'
        );

        $this->channel = $this->broker->channel();

        $this->queue_name_photos = sprintf('queue_photos_b%d', $board);
        $exchange_name = sprintf('idxboost_%d', $board);

        $this->channel->queue_declare(
            $this->queue_name_photos,
            false,
            true,
            false,
            false
        );

        $this->channel->exchange_declare(
            $exchange_name,
            AMQPExchangeType::DIRECT,
            false,
            true,
            false
        );

        $this->channel->queue_bind(
            $this->queue_name_photos,
            $exchange_name
        );


    }


}