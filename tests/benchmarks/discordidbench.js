import b from "benny";

function hashDiscordUserIdCharWise(userId, seed = 0) {
	let hash = seed >>> 0;

	// Process each character
	for (let i = 0; i < userId.length; i++) {
		const charCode = userId.charCodeAt(i);
		hash = (hash * 31 + charCode) >>> 0; // Prime multiplier 31
	}

	// Final mixing
	hash ^= hash >>> 16;
	hash *= 0x85ebca6b;
	hash ^= hash >>> 13;
	hash *= 0xc2b2ae35;
	hash ^= hash >>> 16;

	return hash >>> 0;
}

function hashDiscordUserIdFNV(userId, seed = 0) {
	let hash = 2166136261 ^ seed; // FNV offset basis and XOR with seed

	for (let i = 0; i < userId.length; i++) {
		hash ^= userId.charCodeAt(i);
		hash = (hash * 16777619) >>> 0; // FNV prime
	}

	return hash >>> 0;
}

function hashDiscordUserId(userId, seed = 0) {
	// Ensure the seed is a 32-bit unsigned integer
	seed = seed >>> 0;

	// Convert the user ID to a BigInt
	if (userId.includes("_")) {
		userId = userId.split("_");
		// apply the below for both and combine the results
		let hash = seed;
		for (let i = 0; i < userId.length; i++) {
			const bigintId = BigInt(userId[i]);

			// Split into 32-bit parts
			const low32 = Number(bigintId & 0xffffffffn);
			const high32 = Number((bigintId >> 32n) & 0xffffffffn);

			// Mix with seed

			hash ^= low32;
			hash ^= high32;

			// Bitwise mixing
			hash = (hash ^ (hash >>> 16)) * 0x85ebca6b;
			hash ^= hash >>> 13;
			hash = (hash ^ (hash >>> 13)) * 0xc2b2ae35;
			hash ^= hash >>> 16;
		}
		return hash >>> 0;
	} else {
		const bigintId = BigInt(userId[i]);

		// Split into 32-bit parts
		const low32 = Number(bigintId & 0xffffffffn);
		const high32 = Number((bigintId >> 32n) & 0xffffffffn);

		// Mix with seed
		let hash = seed;
		hash ^= low32;
		hash ^= high32;

		// Bitwise mixing
		hash = (hash ^ (hash >>> 16)) * 0x85ebca6b;
		hash ^= hash >>> 13;
		hash = (hash ^ (hash >>> 13)) * 0xc2b2ae35;
		hash ^= hash >>> 16;
		return hash >>> 0;
	}
}
import xxhash from "xxhash-wasm";
const { h32, h64 } = await xxhash();
// Example usage id1_id2
const discordUserId = "758734482857197568_715755977483223081";
const seed = 42; // Optional seed
const bigseed = BigInt(seed);
await b.suite(
	"Discord User ID Hashing",
	b.add("Char-wise hashing", () => {
		hashDiscordUserIdCharWise(discordUserId, seed);
	}),
	b.add("FNV hashing", () => {
		hashDiscordUserIdFNV(discordUserId, seed);
	}),
	b.add("BigInt hashing", () => {
		hashDiscordUserId(discordUserId, seed);
	}),
	b.add("xxHash32", () => {
		h32(discordUserId, seed);
	}),
	b.add("xxHash64", () => {
		h64(discordUserId, bigseed);
	}),
	b.cycle(),
	b.complete()
);
