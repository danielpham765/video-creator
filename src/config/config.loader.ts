import * as fs from 'fs';
import * as path from 'path';
import yaml from 'js-yaml';

export default function loadYamlConfig() {
  try {
    const cfgPath = path.join(process.cwd(), 'config', 'config.yaml');
    if (!fs.existsSync(cfgPath)) return {};
    const raw = fs.readFileSync(cfgPath, 'utf8');
    const parsed = yaml.load(raw) as Record<string, any> | undefined;
    return parsed || {};
  } catch (err) {
    // On parse/read error, return empty config so env vars still work.
    return {};
  }
}
