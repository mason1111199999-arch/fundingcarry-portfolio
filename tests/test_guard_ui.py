from pathlib import Path
import shutil
import subprocess
import unittest


class GuardCoverageUiTests(unittest.TestCase):
    @unittest.skipUnless(shutil.which("node"), "Node.js is needed to exercise the dashboard JavaScript")
    def test_current_pair_count_and_closed_history_filter(self):
        html = Path(__file__).resolve().parents[1] / "v4_demo" / "fundingcarryv4.html"
        script = r'''
const fs = require('fs');
const assert = require('assert');
const lines = fs.readFileSync(process.argv[1], 'utf8').split('\n');
const wanted = ['function guardMatches(', 'function isGuarded(', 'function isClosedHistoricalGuard('];
for (const prefix of wanted) assert(lines.some(x => x.startsWith(prefix)), prefix);
eval(lines.filter(x => wanted.some(prefix => x.startsWith(prefix))).join('\n'));
const wld = {asset:'ASSET', legs:[{symbol:'A'}, {symbol:'B'}]};
const paused = {asset:'ASSET', state:'manual_reconcile_required', reason:'A position increased', legs:[{symbol:'A'}, {symbol:'B'}], baseline:{A:'10',B:'-10'}};
const closed = {asset:'OLD', state:'manual_reconcile_required', reason:'C baseline is zero', legs:[{symbol:'C'}, {symbol:'D'}], baseline:{C:'0',D:'0'}};
assert(isClosedHistoricalGuard(closed, [{symbol:'A'}, {symbol:'B'}]));
assert(!isClosedHistoricalGuard({...closed, pending_action:{symbol:'C'}}, []));
assert(!isClosedHistoricalGuard(closed, [{symbol:'C'}]));
assert(!isClosedHistoricalGuard({...closed, baseline:{C:null,D:'0'}}, []));
assert(!isGuarded(wld, [paused]));
assert(isGuarded(wld, [{...paused,state:'active'}]));
assert(!isGuarded({asset:'ASSET',legs:[{symbol:'A'}]}, [{...paused,state:'active'}]));
let dash = {connected:true, positions:[{symbol:'A'},{symbol:'B'}], guards:[paused,closed]};
let observed = null;
let render = () => { observed = dash.guards.map(g => g.asset); };
const metric = {textContent:'',parentElement:{querySelector:()=>({textContent:''})}};
const boxes = {guard:metric,pairRisk:{insertAdjacentHTML:()=>{}},riskTop:{insertAdjacentHTML:()=>{}}};
const $ = id => boxes[id];
const groups = () => [wld];
const esc = x => String(x);
const wrapper = lines.find(x => x.startsWith('const renderPositionCoverageBase=render;'));
assert(wrapper);
eval(wrapper);
render();
assert.strictEqual(metric.textContent, '1 / 1');
assert.deepStrictEqual(observed, ['ASSET']);
assert.strictEqual(dash.guards.length, 2);
'''
        result = subprocess.run([shutil.which("node"), "-e", script, str(html)], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
