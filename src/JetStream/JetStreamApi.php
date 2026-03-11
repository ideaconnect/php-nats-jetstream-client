<?php

declare(strict_types=1);

namespace Idct\Nats\JetStream;

final class JetStreamApi
{
    public const ACCOUNT_INFO = '$JS.API.INFO';
    public const STREAM_CREATE_PREFIX = '$JS.API.STREAM.CREATE.';
    public const STREAM_INFO_PREFIX = '$JS.API.STREAM.INFO.';
    public const STREAM_DELETE_PREFIX = '$JS.API.STREAM.DELETE.';
    public const STREAM_MSG_GET_PREFIX = '$JS.API.STREAM.MSG.GET.';
    public const CONSUMER_CREATE_PREFIX = '$JS.API.CONSUMER.CREATE.';
    public const CONSUMER_INFO_PREFIX = '$JS.API.CONSUMER.INFO.';
    public const CONSUMER_DELETE_PREFIX = '$JS.API.CONSUMER.DELETE.';
    public const CONSUMER_MSG_NEXT_PREFIX = '$JS.API.CONSUMER.MSG.NEXT.';
}
