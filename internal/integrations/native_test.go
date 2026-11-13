package integrations

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/evanw/esbuild/pkg/api"
)

func TestNativeHostBoundary(t *testing.T) {
	dir := t.TempDir()
	bundle := api.Build(api.BuildOptions{
		EntryPoints: []string{"../../integrations/native.ts"},
		Bundle:      true,
		Platform:    api.PlatformNode,
		Format:      api.FormatESModule,
		Write:       false,
		LogLevel:    api.LogLevelSilent,
	})
	if len(bundle.Errors) != 0 {
		t.Fatalf("bundle native adapter: %v", bundle.Errors)
	}
	if len(bundle.OutputFiles) != 1 {
		t.Fatalf("expected one adapter bundle, got %d", len(bundle.OutputFiles))
	}
	if err := os.WriteFile(filepath.Join(dir, "native.mjs"), bundle.OutputFiles[0].Contents, 0600); err != nil {
		t.Fatal(err)
	}
	scenarios := []struct{ name, script string }{
		{"snapshots read current getters and exclude message contents", `import { strict as assert } from 'node:assert';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { createSnapshot, NativePipe, registerNative } from './native.js';
await (()=>{
    let mode = 'tui';
    const ctx = {
        cwd: '/work',
        get mode() { return mode; },
        hasUI: true,
        sessionManager: {
            getSessionId: ()=>'session',
            getUsageStatistics: () => { throw new Error('forbidden'); }
        },
        getContextUsage: () => { throw new Error('forbidden'); },
        isIdle: ()=>true,
        ui: {
            notify () {}
        }
    };
    const update = createSnapshot('pi', ctx, {
        message: {
            role: 'assistant',
            content: 'private prompt',
            stopReason: 'stop',
            usage: {
                input: 1,
                output: 2,
                cacheRead: 0,
                cacheWrite: 0,
                totalTokens: 3
            }
        }
    }, 'message_end');
    mode = 'rpc';
    const fresh = createSnapshot('pi', ctx);
    assert.equal(update.mode, 'tui');
    assert.equal(fresh.mode, 'rpc');
    assert.equal(update.event.stopReason, 'stop');
    for (const key of ['context', 'usage', 'aggregate']) assert.ok(!(key in update));
    assert.equal(fresh.event, undefined);
    assert.ok(!JSON.stringify(update).includes('private prompt'));
})();
`},
		{"native snapshots project async counts without private payloads", `import { strict as assert } from 'node:assert';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { createSnapshot, NativePipe, registerNative } from './native.js';
await (()=>{
    const ctx = {
        cwd: '/work',
        mode: 'tui',
        hasUI: true,
        sessionManager: {
            getSessionId: ()=>'session',
            getUsageStatistics: () => { throw new Error('forbidden'); }
        },
        getAsyncJobSnapshot: ()=>({
                running: [
                    {
                        command: 'PRIVATE_COMMAND'
                    },
                    {
                        label: 'PRIVATE_LABEL'
                    }
                ],
                delivery: {
                    queued: 1,
                    delivering: true,
                    pendingJobIds: [
                        'PRIVATE_JOB'
                    ]
                }
            }),
        getContextUsage: () => { throw new Error('forbidden'); },
        isIdle: ()=>false,
        ui: {
            notify () {}
        }
    };
    const event = {
        message: {
            role: 'assistant',
            content: 'PRIVATE_MESSAGE',
            usage: {
                input: 10,
                output: 3,
                cacheRead: 5,
                cacheWrite: 2,
                totalTokens: 20,
                reasoning: 0,
                reasoningTokens: 2
            }
        }
    };
    const omp = createSnapshot('omp', ctx, event, 'message_end');
    assert.deepEqual(omp.asyncJobs, {
        running: 2,
        pending: 1
    });
    const pi = createSnapshot('pi', ctx, event, 'message_end');
    assert.equal(pi.asyncJobs, undefined);
    for (const key of ['context', 'usage', 'aggregate']) {
        assert.ok(!(key in omp));
        assert.ok(!(key in pi));
    }
    for (const secret of [
        'PRIVATE_COMMAND',
        'PRIVATE_LABEL',
        'PRIVATE_JOB',
        'PRIVATE_MESSAGE',
        'subagentCost'
    ])assert.ok(!JSON.stringify(omp).includes(secret));
})();
`},
		{"reverse snapshots stay fresh, cancelled replies are ignored, and pending work is rejected on close", `import { strict as assert } from 'node:assert';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { createSnapshot, NativePipe, registerNative } from './native.js';
await (async ()=>{
    const dir = await mkdtemp(join(tmpdir(), 'coordinator-pipe-'));
    const binary = join(dir, 'child');
    await writeFile(binary, ` + "`" + `#!${process.execPath}
let buffer = '', held;
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => {
  buffer += chunk;
  let end;
  while ((end = buffer.indexOf('\\n')) >= 0) {
    const message = JSON.parse(buffer.slice(0, end)); buffer = buffer.slice(end + 1);
    if (message.method === 'inspect') {
      held = message.id;
      console.log(JSON.stringify({version:1,method:'notice'}));
    }
    if (message.method === 'list') {
      if (message.input.limit === 2) process.exit(0);
      if (held) {
        console.log(JSON.stringify({version:1,id:held,result:'late'}));
        held = undefined;
      }
      console.log(JSON.stringify({version:1,id:message.id,method:'snapshot',ref:{}}));
    }
    if (message.method === 'snapshot') console.log(JSON.stringify({version:1,id:message.id,result:message.snapshot.mode}));
    if (message.method === 'shutdown') process.exit(0);
  }
});` + "`" + `, {
        mode: 0o700
    });
    let mode = 'tui';
    const ctx = {
        cwd: '/work',
        get mode() { return mode; },
        hasUI: true,
        sessionManager: {
            getSessionId: ()=>'session'
        },
        isIdle: ()=>true,
        ui: {
            notify () {}
        }
    };
    const { promise: received, resolve: ready } = Promise.withResolvers();
    const pipe = new NativePipe(binary, 'pi', ()=>createSnapshot('pi', ctx), ()=>ready());
    let exiting;
    const stop = async (childPipe)=>{
        const child = childPipe['child'];
        const exited = child.exitCode === null && child.signalCode === null ? once(child, 'exit') : Promise.resolve();
        childPipe.close();
        await exited;
    };
    try {
        assert.equal(await pipe.request('list', {
            input: {
                limit: 1
            }
        }), 'tui');
        mode = 'rpc';
        assert.equal(await pipe.request('list', {
            input: {
                limit: 1
            }
        }), 'rpc');
        const controller = new AbortController();
        const cancelled = assert.rejects(pipe.request('inspect', {}, controller.signal), /aborted/);
        await received;
        controller.abort();
        await cancelled;
        assert.equal(await pipe.request('list', {
            input: {
                limit: 1
            }
        }), 'rpc');
        const pending = assert.rejects(pipe.request('inspect', {}), /shut down/);
        pipe.close();
        await pending;
        exiting = new NativePipe(binary, 'pi', ()=>createSnapshot('pi', ctx), ()=>{});
        await assert.rejects(exiting.request('list', {
            input: {
                limit: 2
            }
        }), /closed|exited/);
    } finally{
        await Promise.all([
            stop(pipe),
            ...exiting ? [
                stop(exiting)
            ] : []
        ]);
        await rm(dir, {
            recursive: true,
            force: true
        });
    }
})();
`},
		{"native commands display the complete Go list without starting a model turn", `import { strict as assert } from 'node:assert';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { createSnapshot, NativePipe, registerNative } from './native.js';
await (async ()=>{
    const dir = await mkdtemp(join(tmpdir(), 'coordinator-list-'));
    const binary = join(dir, 'child');
    await writeFile(binary, ` + "`" + `#!${process.execPath}
let buffer = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => {
  buffer += chunk;
  let end;
  while ((end = buffer.indexOf('\\n')) >= 0) {
    const message = JSON.parse(buffer.slice(0, end)); buffer = buffer.slice(end + 1);
    if (message.method === 'list') console.log(JSON.stringify({version:1,id:message.id,result:{machines:[],sessions:[{ref:{machineId:'machine',harness:'pi',nativeSessionId:'session'}}],nextCursor:null}}));
    if (message.method === 'shutdown') process.exit(0);
  }
});` + "`" + `, {
        mode: 0o700
    });
    const oldBinary = process.env.COORDINATOR_BINARY;
    process.env.COORDINATOR_BINARY = binary;
    try {
        for (const harness of [
            'pi',
            'omp'
        ]){
            const handlers = new Map();
            let command;
            const messages = [];
            const host = {
                on: (name, handler)=>handlers.set(name, handler),
                registerCommand: (_name, options)=>{
                    command = options.handler;
                },
                registerTool: ()=>{},
                registerMessageRenderer: ()=>{},
                sendMessage: (message, options)=>messages.push({
                        message,
                        options
                    })
            };
            const ctx = {
                cwd: '/work',
                mode: 'rpc',
                hasUI: false,
                sessionManager: {
                    getSessionId: ()=>'session'
                },
                getContextUsage: ()=>undefined,
                isIdle: ()=>true,
                ui: {
                    notify () {}
                }
            };
            registerNative(host, harness, (text)=>text);
            await handlers.get('session_start')({}, ctx);
            await command('list');
            assert.equal(messages.length, 1);
            assert.equal(messages[0].options.triggerTurn, false);
            assert.deepEqual(JSON.parse(messages[0].message.content), messages[0].message.details);
            assert.deepEqual(messages[0].message.details, {
                machines: [],
                sessions: [
                    {
                        ref: {
                            machineId: 'machine',
                            harness: 'pi',
                            nativeSessionId: 'session'
                        }
                    }
                ],
                nextCursor: null
            });
            await handlers.get('session_shutdown')({}, ctx);
        }
    } finally{
        if (oldBinary === undefined) delete process.env.COORDINATOR_BINARY;
        else process.env.COORDINATOR_BINARY = oldBinary;
        await rm(dir, {
            recursive: true,
            force: true
        });
    }
})();
`},
		{"coordinate input preserves images and native result shapes without starting a turn", `import { strict as assert } from 'node:assert';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { createSnapshot, NativePipe, registerNative } from './native.js';
await (async ()=>{
    const dir = await mkdtemp(join(tmpdir(), 'coordinator-coordinate-'));
    const binary = join(dir, 'child');
    await writeFile(binary, ` + "`" + `#!${process.execPath}
let buffer = '';
process.stdin.setEncoding('utf8');
process.stdin.on('data', chunk => {
  buffer += chunk;
  let end;
  while ((end = buffer.indexOf('\\n')) >= 0) {
    const message = JSON.parse(buffer.slice(0, end)); buffer = buffer.slice(end + 1);
    if (message.method === 'list') console.log(JSON.stringify({version:1,id:message.id,result:{machines:[],sessions:[{ref:{machineId:'machine',harness:'omp',nativeSessionId:'fleet-session'}}],nextCursor:null}}));
    if (message.method === 'shutdown') process.exit(0);
  }
});` + "`" + `, {
        mode: 0o700
    });
    const oldBinary = process.env.COORDINATOR_BINARY;
    process.env.COORDINATOR_BINARY = binary;
    try {
        for (const harness of [
            'pi',
            'omp'
        ]){
            const handlers = new Map();
            const messages = [];
            const host = {
                on: (name, handler)=>handlers.set(name, handler),
                registerCommand: ()=>{},
                registerTool: ()=>{},
                registerMessageRenderer: ()=>{},
                sendMessage: (...args)=>messages.push(args)
            };
            const ctx = {
                cwd: '/work',
                mode: 'rpc',
                hasUI: false,
                sessionManager: {
                    getSessionId: ()=>'session'
                },
                getContextUsage: ()=>undefined,
                isIdle: ()=>true,
                ui: {
                    notify () {}
                }
            };
            registerNative(host, harness, (text)=>text);
            await handlers.get('session_start')({}, ctx);
            const input = handlers.get('input');
            for (const text of [
                '/coordinate',
                'please coordinate',
                ' coordinate',
                'coordinates',
                'coordinate_task',
                'coordinate/file',
                'Coordinate'
            ]){
                assert.equal(await input({
                    text
                }, ctx), undefined);
            }
            const images = [
                {
                    type: 'image',
                    data: 'private-image'
                }
            ];
            for (const text of [
                'coordinate',
                'coordinate inspect',
                'coordinate\ninspect'
            ]){
                const result = await input({
                    text,
                    images
                }, ctx);
                assert.ok(result.text.startsWith(text));
                assert.ok(result.text.includes('fleet-session'));
                assert.equal(result.images, images);
                if (harness === 'pi') assert.equal(result.action, 'transform');
                else assert.equal('action' in result, false);
            }
            assert.deepEqual(messages, []);
            await handlers.get('session_shutdown')({}, ctx);
        }
    } finally{
        if (oldBinary === undefined) delete process.env.COORDINATOR_BINARY;
        else process.env.COORDINATOR_BINARY = oldBinary;
        await rm(dir, {
            recursive: true,
            force: true
        });
    }
})();
`},
		{"peer delivery and reply are scoped to the invocation", `import { strict as assert } from 'node:assert';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { registerNative } from './native.js';
const dir = await mkdtemp(join(tmpdir(), 'coordinator-peer-'));
const binary = join(dir, 'child');
await writeFile(binary, ` + "`" + `#!${process.execPath}
const readline = require('node:readline');
const send = x => process.stdout.write(JSON.stringify({version:1,...x})+'\\n');
let count = 0;
readline.createInterface({input:process.stdin}).on('line', line => {
 const f = JSON.parse(line);
 if (f.method === 'update' && !count++) {
   const message = {id:'original',sender:'peer',to:{machineId:'machine',harness:process.argv[4],nativeSessionId:'session'},text:'hello',createdAt:Date.now(),expiresAt:Date.now()+600000,status:'dispatched'};
   send({id:'wrong',method:'message',message:{...message,to:{...message.to,nativeSessionId:'other'}}});
   send({id:'expired',method:'message',message:{...message,id:'expired',expiresAt:Date.now()-1}});
   send({id:'invalid',method:'message',message:{...message,id:'invalid',text:null}});
   send({id:'delivery',method:'message',message});
   send({id:'duplicate',method:'message',message});
 }
 if (f.method === 'message.reply') send({id:f.id,result:{id:'reply',inReplyTo:f.reply.replyId}});
 if (f.method === 'shutdown') process.exit(0);
});
` + "`" + `, {mode:0o700});
const old = process.env.COORDINATOR_BINARY;
process.env.COORDINATOR_BINARY = binary;
try {
 for (const harness of ['pi','omp']) {
  const handlers = new Map(), tools = new Map(), messages = [];
  let session = 'session', idle = harness === 'pi';
  const ctx = {cwd:'/',mode:'tui',hasUI:false,sessionManager:{getSessionId:()=>session},isIdle:()=>idle,ui:{notify(){}}};
  const host = {on:(name,fn)=>handlers.set(name,fn),registerCommand(){},registerTool:t=>tools.set(t.name,t),registerMessageRenderer(){},sendMessage:(m,o)=>messages.push([m,o])};
  registerNative(host,harness,()=>null);
  await handlers.get('session_start')({},ctx);
  await new Promise(resolve=>setTimeout(resolve,250));
  assert.equal(messages.length,1);
  assert.match(messages[0][0].content,/replyID: original/);
  assert.match(messages[0][0].content,/not user approval/);
  assert.equal(messages[0][1].triggerTurn,idle);
  assert.equal(messages[0][1].deliverAs,'followUp');
  const reply = await tools.get('coordinator_message_reply').execute('tool',{replyId:'original',text:'answer'},undefined,undefined,ctx);
  assert.equal(reply.details.inReplyTo,'original');
  const oldCtx = {...ctx,sessionManager:{getSessionId:()=> 'session'}};
  session = 'rotated'; await handlers.get('session_switch')({},ctx);
  await assert.rejects(tools.get('coordinator_message_reply').execute('tool',{replyId:'original',text:'answer'},undefined,undefined,oldCtx));
  await handlers.get('session_shutdown')({},ctx);
 }
} finally { if (old === undefined) delete process.env.COORDINATOR_BINARY; else process.env.COORDINATOR_BINARY=old; await rm(dir,{recursive:true,force:true}); }
`},
	}
	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			script := strings.ReplaceAll(scenario.script, "./native.js", "./native.mjs")
			cmd := exec.CommandContext(ctx, "node", "--input-type=module", "-e", script)
			cmd.Dir = dir
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("node scenario failed: %v\n%s", err, output)
			}
		})
	}
}
