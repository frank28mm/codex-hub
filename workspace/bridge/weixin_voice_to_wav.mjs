#!/usr/bin/env node

import fs from "node:fs/promises";
import path from "node:path";

const SILK_SAMPLE_RATE = 24_000;

function pcmBytesToWav(pcm, sampleRate) {
  const pcmBytes = pcm.byteLength;
  const totalSize = 44 + pcmBytes;
  const buf = Buffer.allocUnsafe(totalSize);
  let offset = 0;

  buf.write("RIFF", offset);
  offset += 4;
  buf.writeUInt32LE(totalSize - 8, offset);
  offset += 4;
  buf.write("WAVE", offset);
  offset += 4;

  buf.write("fmt ", offset);
  offset += 4;
  buf.writeUInt32LE(16, offset);
  offset += 4;
  buf.writeUInt16LE(1, offset);
  offset += 2;
  buf.writeUInt16LE(1, offset);
  offset += 2;
  buf.writeUInt32LE(sampleRate, offset);
  offset += 4;
  buf.writeUInt32LE(sampleRate * 2, offset);
  offset += 4;
  buf.writeUInt16LE(2, offset);
  offset += 2;
  buf.writeUInt16LE(16, offset);
  offset += 2;

  buf.write("data", offset);
  offset += 4;
  buf.writeUInt32LE(pcmBytes, offset);
  offset += 4;

  Buffer.from(pcm.buffer, pcm.byteOffset, pcm.byteLength).copy(buf, offset);
  return buf;
}

async function main() {
  const [, , sourcePath, outputPath] = process.argv;
  if (!sourcePath || !outputPath) {
    throw new Error("usage: weixin_voice_to_wav.mjs <input.silk> <output.wav>");
  }
  const { decode } = await import("silk-wasm");
  const silkBuffer = await fs.readFile(sourcePath);
  const result = await decode(silkBuffer, SILK_SAMPLE_RATE);
  const wavBuffer = pcmBytesToWav(result.data, SILK_SAMPLE_RATE);
  await fs.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.writeFile(outputPath, wavBuffer);
  process.stdout.write(
    JSON.stringify({
      ok: true,
      output_path: outputPath,
      duration_ms: result.duration,
    }),
  );
}

main().catch((error) => {
  process.stderr.write(String(error?.message || error));
  process.exit(1);
});
