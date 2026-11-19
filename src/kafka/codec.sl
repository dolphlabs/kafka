// Batch compression. gzip goes through the stdlib; snappy, lz4 and zstd
// are written in slang (snappy.sl, lz4.sl, zstd.sl).

import "compress";

pub fn codec_name(codec: int) -> str {
    return switch codec {
        case 0 { "none" }
        case 1 { "gzip" }
        case 2 { "snappy" }
        case 3 { "lz4" }
        case 4 { "zstd" }
        default { "codec " + to_str(codec) }
    };
}

// data compressed with codec.
pub fn compress(codec: int, data: bytes) -> result[bytes, str] {
    if codec == CODEC_NONE {
        return ok(data);
    }
    if codec == CODEC_GZIP {
        let r = compress.gzip(data);
        guard let out = r else let e = err_of(r) { return err("kafka: gzip: " + e); }
        return ok(out);
    }
    if codec == CODEC_SNAPPY {
        return ok(snappy_encode(data));
    }
    if codec == CODEC_LZ4 {
        return ok(lz4_encode(data));
    }
    if codec == CODEC_ZSTD {
        return ok(zstd_encode(data));
    }
    return err("kafka: compression codec " + to_str(codec) + " is unknown");
}

// data decompressed, refusing output past max bytes.
pub fn decompress(codec: int, data: bytes, max: int) -> result[bytes, str] {
    if codec == CODEC_NONE {
        return ok(data);
    }
    if codec == CODEC_GZIP {
        let r = compress.gunzip(data, max);
        guard let out = r else let e = err_of(r) { return err("kafka: gzip: " + e); }
        return ok(out);
    }
    if codec == CODEC_SNAPPY {
        return snappy_decode(data, max);
    }
    if codec == CODEC_LZ4 {
        return lz4_decode(data, max);
    }
    if codec == CODEC_ZSTD {
        return zstd_decode(data, max);
    }
    return err("kafka: compression codec " + to_str(codec) + " is unknown");
}
