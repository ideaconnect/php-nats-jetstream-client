<?php

declare(strict_types=1);

namespace IDCT\NATS\JetStream;

/**
 * Constants for JetStream API subject prefixes and helper builders.
 */
final class JetStreamApi
{
    public const ACCOUNT_INFO = '$JS.API.INFO';
    public const STREAM_CREATE_PREFIX = '$JS.API.STREAM.CREATE.';
    public const STREAM_INFO_PREFIX = '$JS.API.STREAM.INFO.';
    public const STREAM_UPDATE_PREFIX = '$JS.API.STREAM.UPDATE.';
    public const STREAM_DELETE_PREFIX = '$JS.API.STREAM.DELETE.';
    public const STREAM_MSG_GET_PREFIX = '$JS.API.STREAM.MSG.GET.';
    public const CONSUMER_CREATE_PREFIX = '$JS.API.CONSUMER.CREATE.';
    public const CONSUMER_INFO_PREFIX = '$JS.API.CONSUMER.INFO.';
    public const CONSUMER_DELETE_PREFIX = '$JS.API.CONSUMER.DELETE.';
    public const CONSUMER_PAUSE_PREFIX = '$JS.API.CONSUMER.PAUSE.';
    public const CONSUMER_LIST_PREFIX = '$JS.API.CONSUMER.LIST.';
    public const CONSUMER_MSG_NEXT_PREFIX = '$JS.API.CONSUMER.MSG.NEXT.';
    public const STREAM_PURGE_PREFIX = '$JS.API.STREAM.PURGE.';
    public const STREAM_LIST = '$JS.API.STREAM.LIST';
    public const STREAM_DIRECT_GET_PREFIX = '$JS.API.DIRECT.GET.';
}
