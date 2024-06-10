import { str, u32, u64 } from "../typings/type.js";


// Hash Functions for Discord Ids
export function hashDiscordIdCharWise(userId: Uint8Array, seed: u32 = 0): u32 {
    let hash = seed >>> 0;

    // Process each character
    for (let i = 0; i < userId.length; i++) {
        hash = (hash * 31 + userId[i]) >>> 0; // Prime multiplier 31
    }

    // Final mixing
    hash ^= (hash >>> 16);
    hash *= 0x85ebca6b;
    hash ^= (hash >>> 13);
    hash *= 0xc2b2ae35;
    hash ^= (hash >>> 16);

    return hash >>> 0;
}

export function hashDiscordUserIdFNV1a(userId: Uint8Array, seed: u32 = 0): u32 {
    let hash = 2166136261 ^ seed;

    for (let i = 0; i < userId.length; i++) {
        hash ^= userId[i];
        hash = (hash * 16777619) >>> 0; 
    }
    return hash >>> 0;
}

export function hashDiscordUserIdBigint(userId: u64 , seed: u32 = 0): u32 {
    const bigintId = BigInt(userId);

    const low32 = Number(bigintId & 0xFFFFFFFFn);
    const high32 = Number((bigintId >> 32n) & 0xFFFFFFFFn);

    let hash = seed;
    hash ^= low32;
    hash ^= high32;

    hash = (hash ^ (hash >>> 16)) * 0x85ebca6b;
    hash ^= (hash >>> 13);
    hash = (hash ^ (hash >>> 13)) * 0xc2b2ae35;
    hash ^= (hash >>> 16);

    return hash >>> 0;
}


/* eslint no-fallthrough: 0 */
/**
 * Mnemonist MurmurHash 3
 * =======================
 *
 * Straightforward implementation of the third version of MurmurHash.
 *
 * Note: this piece of code belong to haschisch.
 */

/**
 * Various helpers.
 */
function mul32(a: u32, b: u32): u32 {
  return (a & 0xffff) * b + (((a >>> 16) * b & 0xffff) << 16) & 0xffffffff;
}

function sum32(a: u32, b: u32): u32 {
  return (a & 0xffff) + (b >>> 16) + (((a >>> 16) + b & 0xffff) << 16) & 0xffffffff;
}

function rotl32(a: u32, b: u32): u32 {
  return (a << b) | (a >>> (32 - b));
}

export function murmurhash3(data: Uint8Array, seed: u32 = 0): u32 {
  var c1 = 0xcc9e2d51,
      c2 = 0x1b873593,
      r1 = 15,
      r2 = 13,
      m = 5,
      n = 0x6b64e654;

  var hash = seed,
      k1,
      i,
      l;

  for (i = 0, l = data.length - 4; i <= l; i += 4) {
    k1 = (
      data[i] |
      (data[i + 1] << 8) |
      (data[i + 2] << 16) |
      (data[i + 3] << 24)
    );

    k1 = mul32(k1, c1);
    k1 = rotl32(k1, r1);
    k1 = mul32(k1, c2);

    hash ^= k1;
    hash = rotl32(hash, r2);
    hash = mul32(hash, m);
    hash = sum32(hash, n);
  }

  k1 = 0;

  switch (data.length & 3) {
    case 3:
      k1 ^= data[i + 2] << 16;
    case 2:
      k1 ^= data[i + 1] << 8;
    case 1:
      k1 ^= data[i];
      k1 = mul32(k1, c1);
      k1 = rotl32(k1, r1);
      k1 = mul32(k1, c2);
      hash ^= k1;
    default:
  }

  hash ^= data.length;
  hash ^= hash >>> 16;
  hash = mul32(hash, 0x85ebca6b);
  hash ^= hash >>> 13;
  hash = mul32(hash, 0xc2b2ae35);
  hash ^= hash >>> 16;

  return hash >>> 0;
};