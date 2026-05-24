package com.distkv.storage;

import com.distkv.model.VersionedValue;

public record StoredEntry(String key, VersionedValue value) {
}
