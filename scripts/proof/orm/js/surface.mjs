import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

function catalogPath() {
  if (process.env.HONKER_ORM_SURFACE) {
    if (!fs.existsSync(process.env.HONKER_ORM_SURFACE)) {
      throw new Error(`HONKER_ORM_SURFACE=${process.env.HONKER_ORM_SURFACE} is not a file`);
    }
    return process.env.HONKER_ORM_SURFACE;
  }
  const here = path.dirname(fileURLToPath(import.meta.url));
  for (const candidate of [
    path.join(here, 'surface.json'),
    path.join(here, '..', 'surface.json'),
  ]) {
    if (fs.existsSync(candidate)) return candidate;
  }
  throw new Error('surface.json not found; set HONKER_ORM_SURFACE');
}

function asInt(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) throw new Error(`expected number, got ${value}`);
  return n;
}

function asText(value) {
  if (value == null) return '';
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  return String(value);
}

function resolve(token, prefix, variables) {
  if (typeof token !== 'string') return token;
  if (token.startsWith('$ns:')) return `${prefix}_${token.slice(4)}`;
  if (token.startsWith('$json:')) {
    const keys = token.slice(6).split(',');
    return JSON.stringify(keys.map((k) => asInt(variables[k])));
  }
  if (token.startsWith('$')) return variables[token.slice(1)];
  return token;
}

function resolveText(text, prefix, variables) {
  let out = text;
  for (const [key, value] of Object.entries(variables)) {
    out = out.replaceAll(`$${key}`, asText(value));
  }
  return out.replaceAll('$ns:', `${prefix}_`);
}

function check(expect, result, prefix, variables) {
  switch (expect.kind) {
    case 'int_gt':
      if (!(asInt(result) > expect.n)) throw new Error(`got ${result}`);
      break;
    case 'int_eq':
      if (asInt(result) !== expect.n) throw new Error(`got ${result}`);
      break;
    case 'int_ge':
      if (!(asInt(result) >= expect.n)) throw new Error(`got ${result}`);
      break;
    case 'int_gt_ref':
      if (!(asInt(result) > asInt(variables[expect.ref]))) throw new Error(`got ${result}`);
      break;
    case 'eq_ref':
      if (asInt(result) !== asInt(variables[expect.ref])) throw new Error(`got ${result}`);
      break;
    case 'json_len': {
      const parsed = JSON.parse(asText(result));
      if (parsed.length !== expect.n) throw new Error(`got ${result}`);
      break;
    }
    case 'json_id_eq_ref': {
      const parsed = JSON.parse(asText(result));
      if (parsed.length !== 1 || asInt(parsed[0].id) !== asInt(variables[expect.ref])) {
        throw new Error(`got ${result}`);
      }
      break;
    }
    case 'contains': {
      const needle = resolveText(expect.s, prefix, variables);
      if (!asText(result).includes(needle)) throw new Error(`${needle} not in ${result}`);
      break;
    }
    case 'empty_string': {
      const text = result == null ? '' : asText(result);
      if (text !== '') throw new Error(`expected empty string, got ${result}`);
      break;
    }
    case 'is_null':
      if (result !== null && result !== undefined) throw new Error(`expected NULL, got ${result}`);
      break;
    default:
      throw new Error(`unknown expect kind ${expect.kind}`);
  }
}

export async function run(scalar, prefix) {
  const catalog = JSON.parse(fs.readFileSync(catalogPath(), 'utf8'));
  const variables = {};
  for (const step of catalog.steps) {
    const args = step.args.map((arg) => resolve(arg, prefix, variables));
    let result;
    try {
      result = await scalar(step.sql, args);
    } catch (err) {
      throw new Error(`${step.id} failed: ${err.message || err}`);
    }
    if (step.store) variables[step.store] = result;
    if (step.expect) {
      try {
        check(step.expect, result, prefix, variables);
      } catch (err) {
        throw new Error(`${step.id}: ${err.message || err}`);
      }
    }
  }
}
