<?php

namespace NeedleProject\LaravelRabbitMq;

use PhpAmqpLib\Channel\AMQPChannel;
use PhpAmqpLib\Connection\AbstractConnection;
use PhpAmqpLib\Connection\AMQPConnectionConfig;
use PhpAmqpLib\Connection\AMQPConnectionFactory;
use PhpAmqpLib\Connection\AMQPSocketConnection;
use PhpAmqpLib\Connection\AMQPStreamConnection;

/**
 * Class AMQPConnection
 *
 * @package NeedleProject\LaravelRabbitMq
 * @author  Adrian Tilita <adrian@tilita.ro>
 */
class AMQPConnection
{
    /**
     * @const array Default connections parameters
     */
    const DEFAULTS = [
        'hostname' => '127.0.0.1',
        'port' => 5672,
        'username' => 'guest',
        'password' => 'guest',
        'vhost' => '/',

        # whether the connection should be lazy
        'lazy' => true,

        'insist' => false,

        # More info about timeouts can be found on https://www.rabbitmq.com/networking.html
        'read_write_timeout' => 3,   // default timeout for writing/reading (in seconds)
        'connect_timeout' => 3,
        'channel_rpc_timeout' => 0,
        'heartbeat' => 0,
        'keep_alive' => false,

        'connection_name' => '',

        'io_type' => AMQPConnectionConfig::IO_TYPE_STREAM,

        'secure' => false,
        'ssl_crypto_method' => STREAM_CRYPTO_METHOD_ANY_CLIENT,
        'ssl_verify' => false,
        'ssl_verify_name' => false,

        'locale' => 'en_US',

        'login_method' => AMQPConnectionConfig::AUTH_AMQPPLAIN,
        'login_response' => null,
    ];

    /**
     * @var array
     */
    protected array $connectionDetails = [];

    /**
     * @var string
     */
    protected string $aliasName = '';

    /**
     * @var null|AbstractConnection
     */
    private ?AbstractConnection $connection = null;

    /**
     * @var null|AMQPChannel
     */
    private ?AMQPChannel $channel = null;

    /**
     * @param string $aliasName
     * @param array $connectionDetails
     * @return AMQPConnection
     */
    public static function createConnection(string $aliasName, array $connectionDetails)
    {
        if ($diff = array_diff(array_keys($connectionDetails), array_keys(self::DEFAULTS))) {
            throw new \InvalidArgumentException(
                sprintf(
                    "Cannot create connection %s, received unknown arguments: %s!",
                    (string) $aliasName,
                    implode(', ', $diff)
                )
            );
        }

        return new static(
            $aliasName,
            array_merge(self::DEFAULTS, $connectionDetails)
        );
    }

    /**
     * AMQPConnection constructor.
     *
     * @param string $aliasName
     * @param array $connectionDetails
     */
    public function __construct(string $aliasName, array $connectionDetails = [])
    {
        $this->aliasName = $aliasName;
        $this->connectionDetails = $connectionDetails;

        if (isset($connectionDetails['lazy']) && $connectionDetails['lazy'] === false) {
            // dummy call
            $this->getConnection();
        }
    }

    /**
     * @return AbstractConnection
     */
    protected function getConnection(): AbstractConnection
    {
        if (is_null($this->connection)) {
            $this->connection = $this->createConnectionByType();
        }

        return $this->connection;
    }

    private function createConnectionByType(): AbstractConnection
    {
        $config = new AMQPConnectionConfig();

        $config->setHost($this->connectionDetails['hostname']);
        $config->setPort($this->connectionDetails['port']);
        $config->setUser($this->connectionDetails['username']);
        $config->setPassword($this->connectionDetails['password']);
        $config->setVhost($this->connectionDetails['vhost']);

        if (!empty($this->connectionDetails['io_type'])) {
            $config->setIoType($this->connectionDetails['io_type']);
        }

        if (!empty($this->connectionDetails['connection_name'])) {
            $config->setConnectionName($this->connectionDetails['connection_name']);
        }

        if (isset($this->connectionDetails['connect_timeout'])) {
            $config->setConnectionTimeout((float)$this->connectionDetails['connect_timeout']);
        }

        if (isset($this->connectionDetails['read_write_timeout'])) {
            $config->setReadTimeout((float)$this->connectionDetails['read_write_timeout']);
            $config->setWriteTimeout((float)$this->connectionDetails['read_write_timeout']);

        }

        if (isset($this->connectionDetails['channel_rpc_timeout'])) {
            $config->setChannelRPCTimeout((float)$this->connectionDetails['channel_rpc_timeout']);
        }

        if (isset($this->connectionDetails['keep_alive'])) {
            $config->setKeepalive((bool)$this->connectionDetails['keep_alive']);
        }

        if (isset($this->connectionDetails['heartbeat'])) {
            $config->setHeartbeat((int)$this->connectionDetails['heartbeat']);
        }

        if (isset($this->connectionDetails['secure'])) {
            $config->setIsSecure((bool)$this->connectionDetails['secure']);
        }

        if (isset($this->connectionDetails['ssl_crypto_method'])) {
            $config->setSslCryptoMethod($this->connectionDetails['ssl_crypto_method']);
        }

        if (isset($this->connectionDetails['ssl_verify_name'])) {
            $config->setSslVerifyName((bool)$this->connectionDetails['ssl_verify_name']);
        }

        if (isset($this->connectionDetails['ssl_verify'])) {
            $config->setSslVerify((bool)$this->connectionDetails['ssl_verify']);
        }

        if (isset($this->connectionDetails['lazy'])) {
            $config->setIsLazy((bool)$this->connectionDetails['lazy']);
        }

        if (isset($this->connectionDetails['insist'])) {
            $config->setInsist((bool)$this->connectionDetails['insist']);
        }

        if (!empty($this->connectionDetails['locale'])) {
            $config->setLocale($this->connectionDetails['locale']);
        }

        if (!empty($this->connectionDetails['login_response'])) {
            $config->setLoginResponse($this->connectionDetails['login_response']);
        }

        return AMQPConnectionFactory::create($config);
    }

    /**
     * Reconnect
     */
    public function reconnect(): void
    {
        $this->getConnection()->channel()->close();
        $this->channel = null;
        $this->getConnection()->reconnect();
    }


    public function getChannel(): ?AMQPChannel
    {
        if (is_null($this->channel)) {
            $this->channel = $this->getConnection()->channel();
        }

        return $this->channel;
    }

    /**
     * Retrieve the connection alias name
     *
     * @return string
     */
    public function getAliasName(): string
    {
        return $this->aliasName;
    }
}
