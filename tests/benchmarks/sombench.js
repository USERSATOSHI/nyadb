import b from "benny";
const time = Date.now();
import { timestampToUint8ArrayLE as timestampToUint8Array,ValueToDataType  } from "../../dist/utils/dataType.js";

export function optValueToDataType(input, type) {
    if (type.startsWith("str:")) {
        return new TextEncoder().encode(input);
    } else {
        const num = input;
        switch (type) {
            case "i8":
            case "u8":
            case "bool":
                return new Uint8Array([num & 0xff]);

            case "i16":
            case "u16":
                return new Uint8Array([(num >> 8) & 0xff, num & 0xff]);

            case "i32":
            case "u32":
                return new Uint8Array([
                    (num >> 24) & 0xff,
                    (num >> 16) & 0xff,
                    (num >> 8) & 0xff,
                    num & 0xff
                ]);

            case "i64":
            case "u64":
                const bigInt = BigInt(num);
                return new Uint8Array([
                    Number((bigInt >> 56n) & 0xffn),
                    Number((bigInt >> 48n) & 0xffn),
                    Number((bigInt >> 40n) & 0xffn),
                    Number((bigInt >> 32n) & 0xffn),
                    Number((bigInt >> 24n) & 0xffn),
                    Number((bigInt >> 16n) & 0xffn),
                    Number((bigInt >> 8n) & 0xffn),
                    Number(bigInt & 0xffn)
                ]);

            case "f32": {
                const buffer = new ArrayBuffer(4);
                const view = new DataView(buffer);
                view.setFloat32(0, num, true);
                return new Uint8Array(buffer);
            }

            case "f64": {
                const buffer = new ArrayBuffer(8);
                const view = new DataView(buffer);
                view.setFloat64(0, num, true);
                return new Uint8Array(buffer);
            }

            default:
                throw new Error(`Invalid data type: ${type}`);
        }
    }
}


const input = 0x12345678;
b.suite(
	"optValueToDataType vs valueToDataType",
	b.add("optValueToDataType", () => {
		optValueToDataType(input, "u32");
	}),
	b.add("valueToDataType", () => {
		ValueToDataType(input, "u32");
	}),
	b.cycle(),
	b.complete(),
	b.save({ file: "u32-to-u8", format: "chart.html" }),
	b.save({ file: "u32-to-u8", format: "json" })
);
