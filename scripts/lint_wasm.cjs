// Official Ruff API, identical selected rules to pyproject.toml; optional Windows fallback.
const fs = require('node:fs');
const path = require('node:path');
const { Workspace, PositionEncoding } = require('../.tools/ruff-wasm/package/ruff_wasm.js');
const root = path.resolve(__dirname, '..');
const ws = new Workspace({
  'line-length': 100, 'target-version': 'py312',
  lint: { select: ['E','F','I','UP','B'], ignore: ['UP012'],
          isort: { 'known-first-party': ['mobilityops'] } },
}, PositionEncoding.Utf16);
const format = process.argv.includes('--format');
const fix = process.argv.includes('--fix');
function files(dir) {
  return fs.readdirSync(dir, {withFileTypes: true}).flatMap(entry => {
    const file = path.join(dir, entry.name);
    return entry.isDirectory() ? (entry.name === '__pycache__' ? [] : files(file)) :
      (entry.name.endsWith('.py') ? [file] : []);
  });
}
let diagnostics = [];
for (const file of ['src','tests','app','scripts','airflow/dags'].flatMap(dir =>
  fs.existsSync(path.join(root,dir)) ? files(path.join(root,dir)) : [])) {
  let source = fs.readFileSync(file, 'utf8');
  if (format) source = ws.format(source);
  let found = ws.check(source);
  if (fix) {
    // WASM does not expose applicability; limit fixes to these reviewed safe rule codes.
    for (let iteration=0; iteration<8; iteration++) {
      const safeCodes = new Set(['I001','F401','UP035','UP006','UP007','UP045','UP017']);
      const edits = found.filter(d => d.fix && safeCodes.has(d.code))
        .flatMap(d => d.fix.edits);
      if (!edits.length) break;
      // API positions are line/column; convert using UTF16 JS string offsets.
      const offsets = (p) => source.split('\n').slice(0,p.row-1)
        .reduce((n,line)=>n+line.length+1,0) + p.column-1;
      const changes = edits.map(e=>({start: offsets(e.location),end: offsets(e.end_location),
                                    content:e.content || ''})).sort((a,b)=>b.start-a.start);
      let bound = source.length+1;
      for (const e of changes) {
        if (e.end<=bound) {
          source=source.slice(0,e.start)+e.content+source.slice(e.end);
          bound=e.start;
        }
      }
      found=ws.check(source);
    }
  }
  if (format || fix) fs.writeFileSync(file,source,'utf8');
  diagnostics.push(...found.map(d=>({file:path.relative(root,file),...d})));
}
const report = {runtime:'official Ruff WASM',version:'0.16.10',diagnostics};
fs.mkdirSync(path.join(root,'reports/evidence'), {recursive:true});
fs.writeFileSync(path.join(root,'reports/evidence/ruff.json'),JSON.stringify(report,null,2));
console.log(JSON.stringify(report,null,2));
process.exitCode=diagnostics.length ? 1 : 0;
