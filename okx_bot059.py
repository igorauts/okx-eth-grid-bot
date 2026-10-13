#!/usr/bin/env python3
"""OKX ETH grid v0.5.9: recover asynchronous BUY cancels without false stops.

Never assumes a TP has completed without a terminal filled SELL order that can
be linked to its attached algo client ID. Unknown states fail closed.
"""
import base64
import hashlib
import hmac
import json
import os
import copy
import secrets
import sys
import fcntl
import shutil
import threading
import queue
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR, ROUND_HALF_UP
from pathlib import Path

import websocket

ROOT = Path(__file__).resolve().parent
CFG = json.loads((ROOT / 'config.json').read_text())
DATA = ROOT / 'state' / 'bot04_state.json'
EVENTS = ROOT / 'logs' / 'bot04_events.jsonl'
INST = 'ETH-USDT-SWAP'
STEP, TP, QTY = Decimal('0.25'), Decimal('1.75'), Decimal('0.01')
TARGET, CAP, EX_CAP = 25, 500, 500
INTERVAL = 1.0  # main safety loop; BID updates independently via public WS
VER = '0.5.9'
REAL_TP_TARGET = 75
REDUCE_ONLY_HARD_CAP = 150
BUY_TP_RESERVE = 25
TP_ACTIONS_PER_PASS = 5
TP_JOURNAL = ROOT / 'logs' / 'tp_transitions_v057.jsonl'
PNL_DATA = ROOT / 'state' / 'bot05_pnl.json'
CONTRACT_VALUE = Decimal('0.1')  # ETH per contract (verified dynamically at startup)
PRICE_STEP = STEP
USDT_QUANT = Decimal('0.000001')

def die(s):
    raise RuntimeError(s)

def fmt(x):
    return f'{x:.2f}'

def D(v):
    return Decimal(str(v or '0'))

if (CFG.get('instrument') != INST or CFG.get('direction') != 'long'
    or CFG.get('td_mode') != 'cross' or CFG.get('buy_type') != 'post_only'
    or CFG.get('tp_type') != 'attached_limit'
    or D(CFG.get('grid_step')) != STEP or D(CFG.get('take_profit')) != TP
    or D(CFG.get('order_size')) != QTY
    or (CFG.get('active_buy_orders') != TARGET and not (os.getenv('OKX_BOT05_STATS')=='ONCE' and CFG.get('active_buy_orders')==20)) or CFG.get('max_grids') != CAP
    or CFG.get('max_exchange_orders') != EX_CAP):
    die('Configuration mismatch. Refusing to start.')

ENV = {}
for line in (ROOT / '.env').read_text().splitlines():
    line = line.strip()
    if line and not line.startswith('#') and '=' in line:
        k,v=line.split('=',1); ENV[k.strip()]=v.strip().strip('"').strip("'")
if not all(ENV.get(k) for k in ('OKX_API_KEY','OKX_API_SECRET','OKX_API_PASSPHRASE')):
    die('Missing API secrets')
if ENV.get('OKX_DEMO','0') != '0':
    die('Unexpected demo mode')

# Intentional separate live activation guard. Setting config alone is NOT enough.
# Audit mode may be used even when live_trading=true in the config.
# It cannot submit or cancel orders: API.req prevents non-GET methods.
STATS_ONLY = os.getenv('OKX_BOT05_STATS') == 'ONCE'
AUDIT = os.getenv('OKX_BOT04_AUDIT') == 'READ_ONLY' or STATS_ONLY
LIVE = (not AUDIT and CFG.get('live_trading') is True
        and os.getenv('OKX_BOT04_LIVE') == 'CONFIRM')
FULL = os.getenv('OKX_BOT04_PROBE') == 'FULL_25'
if CFG.get('live_trading') is True and not LIVE and not AUDIT:
    die('Live config requires explicit OKX_BOT04_LIVE=CONFIRM')
if LIVE and not FULL:
    die('LIVE requires OKX_BOT04_PROBE=FULL_25 for the 25-order bot.')


def atomic_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_name(path.name + '.tmp')
    fd = os.open(tmp, os.O_WRONLY|os.O_CREAT|os.O_TRUNC, 0o600)
    with os.fdopen(fd,'w') as f:
        json.dump(value, f, indent=2, ensure_ascii=False)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp,path)
    dirfd = os.open(path.parent, os.O_RDONLY)
    try: os.fsync(dirfd)
    finally: os.close(dirfd)

def log(event, **details):
    record={'ts':datetime.now(timezone.utc).isoformat(),'event':event,**details}
    fd=os.open(EVENTS,os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
    with os.fdopen(fd,'a') as f: f.write(json.dumps(record,ensure_ascii=False)+'\n')
    t = record['ts'][11:19]
    # Full structured events remain in logs/bot04_events.jsonl, even when
    # we suppress redundant terminal messages from the WebSocket stream.
    if event == 'WS_ORDER':
        return
    if event == 'BUY_PLACED':
        print(f"📌 BUY {details['price']} → TP {details['tp']} | LIMIT", flush=True)
    elif event == 'BUY_FILLED':
        print(f"💥 Вход {details['price']} исполнен → ждём TP {details['tp']}",flush=True)
    elif event == 'TP_FILLED':
        # Money lines follow when individual exchange fills can be verified.
        pass
    elif event == 'TP_LINKED':
        print(f"🔗 TP {details['price']} привязан | SELL {details['tpOrdId']}",flush=True)
    elif event == 'BUY_MOVED_UP':
        print(f"↗️ Сетка вверх | отменён BUY {details['cancelled']}",flush=True)
    elif event == 'POSITION_SYNC_WAIT':
        print(f"⏳ Сверка позиции: биржа {details['exchange']} / бот {details['local']}",flush=True)
    elif event == 'POSITION_SYNC_OK':
        print(f"✅ Позиция синхронизирована: {details['exchange']}",flush=True)
    else:
        print(f"[{t}] {event}",details,flush=True)

class RateLimitedError(RuntimeError):
    """OKX 429/50011 persisted after safe GET-only retries."""


class RestGovernor:
    """One shared in-process gate for trading, PnL and full statistics.

    The 3-month fills-history endpoint permits 10 requests / 2 seconds per
    OKX user. Use 0.55s between requests across BOTH analytics threads,
    including other instances of API in this process. Trading queries and
    order writes do not wait for an analytics network request to complete.
    NOTE: another process using the same OKX user is not governed here.
    """
    def __init__(self):
        self.cv=threading.Condition()
        self.next_endpoint={}
        self.cooldown={}
        self.last_any_get=0.0
        self.urgent_inflight=0
        self.last_urgent=0.0

    def begin(self, method, route, background=False):
        urgent=not background
        if urgent:
            with self.cv:
                self.urgent_inflight+=1
                self.last_urgent=time.monotonic()
                self.cv.notify_all()
        with self.cv:
            while True:
                now=time.monotonic()
                delay=max(0.0, self.cooldown.get(route,0)-now,
                          self.next_endpoint.get(route,0)-now)
                if method=='GET':
                    delay=max(delay, self.last_any_get+0.045-now)
                if background:
                    # Yield REST capacity to order/position reconciliation.
                    if self.urgent_inflight:
                        delay=max(delay,0.15)
                    elif now-self.last_urgent<0.15:
                        delay=max(delay,0.15-(now-self.last_urgent))
                if delay>0:
                    self.cv.wait(timeout=delay)
                    continue
                if method=='GET':self.last_any_get=now
                if route=='/api/v5/trade/fills-history':
                    self.next_endpoint[route]=now+0.55
                break
        return urgent

    def end(self, urgent):
        if urgent:
            with self.cv:
                self.urgent_inflight-=1
                self.last_urgent=time.monotonic()
                self.cv.notify_all()

    def throttle(self, route, seconds):
        with self.cv:
            self.cooldown[route]=max(self.cooldown.get(route,0),
                                    time.monotonic()+seconds)
            self.cv.notify_all()

REST_GATE=RestGovernor()


class API:
    def req(self, method, path, params=None, body=None, *, background=False):
        if method != 'GET' and not LIVE:
            die('Write operations disabled in DRY RUN')
        if params: path += '?' + urllib.parse.urlencode(params)
        route=path.split('?',1)[0]
        payload=json.dumps(body,separators=(',',':')) if body is not None else ''
        # Re-sign EACH attempt (timestamps can expire while waiting for a slot).
        for attempt in range(3 if method=='GET' else 1):
            urgent=REST_GATE.begin(method,route,background=background)
            limited=False; error_text=''
            try:
                ts=datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
                msg=ts+method+path+payload
                sig=base64.b64encode(hmac.new(ENV['OKX_API_SECRET'].encode(),msg.encode(),hashlib.sha256).digest()).decode()
                headers={'User-Agent':'curl/8.5.0','Content-Type':'application/json',
                    'OK-ACCESS-KEY':ENV['OKX_API_KEY'],'OK-ACCESS-SIGN':sig,
                    'OK-ACCESS-TIMESTAMP':ts,'OK-ACCESS-PASSPHRASE':ENV['OKX_API_PASSPHRASE']}
                req=urllib.request.Request('https://www.okx.com'+path,
                    data=payload.encode() if body is not None else None,
                    method=method,headers=headers)
                try:
                    with urllib.request.urlopen(req,timeout=12) as r: result=json.load(r)
                except urllib.error.HTTPError as e:
                    error_text=e.read(400).decode(errors='replace')
                    if e.code==429 or '"code":"50011"' in error_text.replace(' ',''):
                        limited=True
                    else:
                        die(f'HTTP {e.code} {error_text}')
                except Exception as e:
                    # Non-GET outcomes MUST NOT be retried without reconciliation.
                    die(f'UNKNOWN REQUEST OUTCOME: {type(e).__name__}: {e}')
                else:
                    if result.get('code') == '50011':
                        limited=True;error_text=result.get('msg','rate limit')
                    elif result.get('code') == '51603' and method == 'GET' and route=='/api/v5/trade/order':
                        return []
                    elif result.get('code')!='0':
                        die(f"OKX API {result.get('code')}: {result.get('msg')}")
                    else:
                        return result.get('data',[])
            finally:
                REST_GATE.end(urgent)
            if limited:
                wait=min(12,2*(2**attempt))
                REST_GATE.throttle(route,wait)
                # Never retry order submission or cancellation automatically.
                if method!='GET' or attempt==2:
                    raise RateLimitedError(f'OKX 50011/429 on {route}; '
                                           'rate limit, no automatic writes retried')
                if not background:
                    print(f'⏳ OKX 429: GET {route} повтор после {wait}s',flush=True)
        die('Unreachable API retry branch')

    def pending(self):
        # Return ALL relevant orders; fail closed if pagination looks strange.
        result=[]; after=None
        for _ in range(8):
            p={'instType':'SWAP','instId':INST,'limit':'100'}
            if after: p['after']=after
            page=self.req('GET','/api/v5/trade/orders-pending',p)
            result.extend(page)
            if len(page)<100: return result
            nxt=page[-1].get('ordId')
            if not nxt or nxt==after: die('Cannot paginate open orders')
            after=nxt
        die('Pending-order pagination overflow')

    def order(self, oid='', cid=''):
        p={'instId':INST}
        if oid: p['ordId']=oid
        else: p['clOrdId']=cid
        data=self.req('GET','/api/v5/trade/order',p)
        return data[0] if data else None

    def positions(self):
        return self.req('GET','/api/v5/account/positions',{'instId':INST})

    def ticker(self):
        data=self.req('GET','/api/v5/market/ticker',{'instId':INST})
        if not data: die('Ticker missing')
        return D(data[0]['bidPx'])

    def instrument(self):
        data=self.req('GET','/api/v5/public/instruments',{'instType':'SWAP','instId':INST})
        if len(data)!=1: die('Instrument missing')
        v=data[0]
        if v.get('state')!='live' or STEP%D(v['tickSz']) or TP%D(v['tickSz']) or QTY<D(v['minSz']) or QTY%D(v['lotSz']):
            die('Instrument specs incompatible')
        return v

    def history(self):
        # Only use this for strong, exact TP ID correlation, never price matching.
        return self.req('GET','/api/v5/trade/orders-history',{'instType':'SWAP','instId':INST,'limit':'100'})

    def fills_by_order(self, order_id):
        """Actual fill records, filtered by exact exchange ordId.

        History covers ~3 months. Never calculate profit from a limit price or
        an order-summary fee estimate. A fully populated page requires a
        separate paginated implementation, so fail PnL closed in that case.
        """
        result=self.req('GET','/api/v5/trade/fills-history',{
            'instType':'SWAP','instId':INST,'ordId':order_id,'limit':'100'
        },background=True)
        if len(result)>=100:
            raise RuntimeError('100+ fills for one order; detailed pagination required')
        if any(str(f.get('ordId'))!=str(order_id) or f.get('instId')!=INST
               for f in result):
            raise RuntimeError('Order fill filter mismatch; financial record unsafe')
        return result

    def algo(self,cid):
        # The attached algo is only registered after parent BUY fully fills.
        data=self.req('GET','/api/v5/trade/order-algo',{'algoClOrdId':cid})
        return data[0] if data else None

A=API()

def newid(prefix):
    return 'OX4'+prefix+str(int(time.time()*1000))+secrets.token_hex(4)

def save(state):
    atomic_json(DATA,state)

def count(state):
    return sum(x['state'] not in ('CLOSED','CANCELED') for x in state['cycles'].values())

def active_buy(state):
    return [x for x in state['cycles'].values() if x['state'] in ('BUY_LIVE','BUY_PARTIAL','PLACING')]

def level_free(state,price):
    key=fmt(price)
    return key not in state['cycles'] or state['cycles'][key]['state'] in ('CLOSED','CANCELED')

def money(value, places=6):
    x=Decimal(str(value)).quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)
    return ('+' if x>=0 else '−') + f'{abs(x):.{places}f}'


def money_usdt(value, places=6):
    signed=money(value,places)
    return signed[0]+'$'+signed[1:]


def bold(value):
    return ('\033[1m'+value+'\033[0m') if sys.stdout.isatty() else value


class PnLLedger:
    """Non-trading cycle PnL ledger. All entries sourced from real OKX fills.

    Separate state file prevents a stats bug from mutating trading positions.
    Stored cycle keys are unique BUY+TP IDs; rearming a grid never double-counts.
    """
    def __init__(self, contract_value):
        self.contract_value=contract_value
        self.lock=threading.RLock()
        if self.contract_value<=0: die('Invalid contract size for profit accounting')
        if PNL_DATA.exists():
            self.data=json.loads(PNL_DATA.read_text())
            if (self.data.get('schema')!=1 or self.data.get('instrument')!=INST):
                die('PnL ledger version/instrument mismatch')
        else:
            self.data={'schema':1,'instrument':INST,'accounted':{}}
        self.tried={}
        self.retry_after={}
        self.recent_total=self.total()
        self.initial_ids=set()

    def total(self):
        with self.lock:
            return sum((D(p['net']) for p in self.data['accounted'].values()),Decimal(0))

    def recorded(self, cycle):
        with self.lock:
            return self.identity(cycle) in self.data['accounted']

    @staticmethod
    def identity(cycle):
        if not cycle.get('ordId'):return None
        legs=[r for r in cycle.get('tpOrders',[]) if D(r.get('filled'))>0]
        if len(cycle.get('tpOrders',[]))>1:
            # Stable across TP replacement; distinct from pre-v0.5.7 ledger keys.
            return 'chain:'+str(cycle['ordId'])
        if not legs and not cycle.get('tpOrdId'):return None
        oid=(legs[0]['ordId'] if legs else cycle.get('tpOrdId'))
        return str(cycle['ordId'])+':'+str(oid)

    def all_cycles(self, st):
        # Includes previously replaced grid levels: completed cycles are
        # archived by place() so their realized PnL must never disappear.
        previous=list(st.get('archived_cycles',[]))
        current=list(st['cycles'].values())
        return [x for x in previous+current if x.get('state')=='CLOSED']

    @staticmethod
    def aggregate(fills, oid, size, side):
        if not fills: raise ValueError('Fills not yet available')
        q=Decimal('0'); amt=Decimal('0'); fee=Decimal('0'); liquidity=set(); ids=set()
        for f in fills:
            if str(f.get('ordId'))!=str(oid): raise ValueError('Unexpected fill order ID')
            if f.get('side') != side: raise ValueError('Wrong side for order fills')
            tid=f.get('tradeId')
            if not tid or tid in ids: raise ValueError('Missing/duplicate tradeId')
            ids.add(tid)
            fs=D(f.get('fillSz')); fp=D(f.get('fillPx'))
            if fs<=0 or fp<=0: raise ValueError('Invalid fill price or size')
            if f.get('feeCcy')!='USDT':
                raise ValueError('Commission currency not USDT; cannot calculate USDT net')
            if f.get('fee') in (None,''): raise ValueError('Missing actual exchange fee')
            if f.get('execType') not in ('M','T'):
                raise ValueError('Missing Maker/Taker execution type')
            q+=fs; amt+=fs*fp; fee+=D(f['fee']);liquidity.add(f['execType'])
        if q!=size: raise ValueError(f'Fill volume differs: {q} != {size}')
        return {'qty':q,'avg_px':amt/q,'fee':fee,'liquidity':'/'.join(sorted(liquidity))}

    def compute(self, cycle, buy_fills, sell_fills):
        amount=D(cycle.get('filledBuy'))
        if amount<=0 or amount!=D(cycle.get('filledTP')):
            raise ValueError('Incomplete cycle cannot have realized profit')
        b=self.aggregate(buy_fills,cycle['ordId'],amount,'buy')
        grouped={}
        for f in sell_fills:
            grouped.setdefault(str(f.get('ordId')),[]).append(f)
        legs=[r for r in cycle.get('tpOrders',[])
              if D(r.get('filled'))>0]
        if not legs:
            legs=[{'ordId':cycle['tpOrdId'],'filled':str(amount)}]
        sold_qty=Decimal(0); sold_amt=Decimal(0); sell_fee=Decimal(0); liq=set()
        for leg in legs:
            oid=leg['ordId']; qty=D(leg['filled'])
            x=self.aggregate(grouped.get(str(oid),[]),oid,qty,'sell')
            sold_qty+=qty;sold_amt+=x['avg_px']*qty
            sell_fee+=x['fee'];liq.update(x['liquidity'].split('/'))
        if sold_qty!=amount:
            raise ValueError('SELL chain volume mismatch')
        sell_avg=sold_amt/sold_qty
        base_qty=amount*self.contract_value
        gross=(sell_avg-b['avg_px'])*base_qty
        plan=(D(cycle['tpPx'])-D(cycle['buyPx']))*base_qty
        execution=gross-plan
        fees=b['fee']+sell_fee
        net=gross+fees
        return {'buy_id':cycle['ordId'],'sell_id':legs[-1]['ordId'],
                'sell_ids':[x['ordId'] for x in legs],
                'buy_px':str(b['avg_px']),'sell_px':str(sell_avg),
                'qty_contracts':str(amount),'qty_eth':str(base_qty),
                'gross':str(gross),'planned_gross':str(plan),
                'execution':str(execution),'fees':str(fees),'net':str(net),
                'buy_liquidity':b['liquidity'],'sell_liquidity':'/'.join(sorted(liq)),
                'accounted_at':datetime.now(timezone.utc).isoformat()}

    def print_closed(self, result, historical=False):
        buy=D(result['buy_px']); sell=D(result['sell_px'])
        steps=(sell-buy)/STEP
        total=self.total()
        earned=bold(money_usdt(result['net']))
        overall=bold(money_usdt(total))
        note=' | история' if historical else ''
        print(f"💰 TP {buy:.2f} → {sell:.2f} | {money(steps,2)} шага "
              f"| в копилку {earned} | всего {overall}{note}",flush=True)
        print(f"   факт {money(result['gross'])} "
              f"(план {money(result['planned_gross'])}, "
              f"execution {money(result['execution'])}, "
              f"комиссия {money(result['fees'])}) "
              f"| {result['buy_liquidity']}/{result['sell_liquidity']}",flush=True)

    def commit_completed(self, cycle, result, historical=False):
        """Durable, idempotent PnL commit across analytics threads."""
        key=self.identity(cycle)
        with self.lock:
            if key in self.data['accounted']:
                return False
            updated={'schema':1,'instrument':INST,
                     'accounted':{**self.data['accounted'],key:result}}
            atomic_json(PNL_DATA,updated)
            self.data=updated
            self.tried.pop(key,None)
            self.retry_after.pop(key,None)
            self.print_closed(result,historical=historical)
            return True

    def ingest_history(self, st, fills):
        """Re-use fills already fetched by the full statistics report.

        No extra REST traffic. Only CLOSE cycles with complete matched BUY/TP
        fills are committed; never infer a missing commission or quantity.
        """
        indexed={}
        for f in fills:
            indexed.setdefault(str(f.get('ordId')),[]).append(f)
        recovered=0
        for c in self.all_cycles(st):
            if not self.identity(c) or self.recorded(c):continue
            try:
                sell_ids=[r['ordId'] for r in c.get('tpOrders',[]) if D(r.get('filled'))>0]
                if not sell_ids:sell_ids=[c['tpOrdId']]
                all_sell=[f for oid in sell_ids for f in indexed.get(str(oid),[])]
                result=self.compute(c,indexed.get(str(c['ordId']),[]),all_sell)
            except ValueError:
                continue  # Late fills: leave for normal worker retry.
            if self.commit_completed(c,result,historical=True):
                recovered+=1
        return recovered

    def work(self, st, max_cycles=1, historical=True):
        # Read-only fill requests. All other threads may keep trading; only
        # the small file commit is serialized with full-stats ingestion.
        pending=[c for c in self.all_cycles(st) if self.identity(c) and not self.recorded(c)]
        if not pending:return
        pending.sort(key=lambda x: str(x.get('tpOrdId','')),reverse=True)
        checked=0
        for c in pending:
            key=self.identity(c)
            now=time.monotonic()
            with self.lock:
                if now<self.retry_after.get(key,0): continue
            try:
                buy=A.fills_by_order(c['ordId'])
                sell_ids=[r['ordId'] for r in c.get('tpOrders',[]) if D(r.get('filled'))>0]
                if not sell_ids:sell_ids=[c['tpOrdId']]
                sell=[f for oid in sell_ids for f in A.fills_by_order(oid)]
                r=self.compute(c,buy,sell)
                self.commit_completed(c,r,historical=key in self.initial_ids)
            except Exception as e:
                with self.lock:
                    count=self.tried.get(key,0)+1
                    self.tried[key]=count
                    wait=min(120,5*count)
                    if isinstance(e,RateLimitedError):wait=max(wait,30)
                    self.retry_after[key]=time.monotonic()+wait
                bp=c.get('buyPx','?');tp=c.get('tpPx','?')
                transient=(isinstance(e,RateLimitedError) or
                    (isinstance(e,ValueError) and
                     ('Fills not yet available' in str(e) or
                      'Fill volume differs' in str(e))))
                if count==1:
                    print(f'⏳ TP {bp} → {tp} закрыт | ждём комиссии',flush=True)
                if (not transient and (count==1 or count%10==0)):
                    print(f'⚠️ PnL {bp} → {tp}: данные требуют проверки: '
                          f'{type(e).__name__} {str(e)[:120]}',flush=True)
                elif isinstance(e,RateLimitedError) and count==1:
                    print('⏳ OKX ограничил запросы истории; PnL продолжит расчёт позже',flush=True)
            checked+=1
            if checked>=max_cycles:break

    def status(self, st):
        unaccounted=sum(bool(self.identity(c)) and not self.recorded(c)
                        for c in self.all_cycles(st))
        return f'всего {money_usdt(self.total())} | PnL не рассчитан {unaccounted}'


def run_tests():
    # Exact rule requested: a partially filled TP NEVER releases its level.
    demo={'cycles': {'2500.00':{'state':'TP_PARTIAL'}}}
    assert not level_free(demo,Decimal('2500'))
    demo['cycles']['2500.00']['state']='TP_WAIT'
    assert not level_free(demo,Decimal('2500'))
    demo['cycles']['2500.00']['state']='CLOSED'
    assert level_free(demo,Decimal('2500'))
    assert grid_top(Decimal('2522.62'))==Decimal('2522.50')
    assert grid_top(Decimal('2522.50'))==Decimal('2522.25')
    desired=desired_buy_levels(Decimal('2522.50'),set())
    assert len(desired)==TARGET
    assert min(desired)==Decimal('2516.50')
    assert max(desired)==Decimal('2522.50')
    assert Decimal('2500.00') not in desired_buy_levels(
        Decimal('2500.50'),{Decimal('2500.00')})
    c={'cycleId':'X','buyPx':'2500.00','tpPx':'2501.75','ordId':'B1',
       'filledBuy':'0.03','filledTP':'0.01','state':'VIRTUAL_TP',
       'tpOrders':[{'ordId':'S1','clOrdId':'old','sz':'0.03',
                    'filled':'0.01','phase':'CANCELED'}]}
    assert remaining_tp(c)==Decimal('0.02')
    assert not active_tp_records(c)
    c['tpOrders'].append({'ordId':'S2','clOrdId':'new','sz':'0.02',
                          'filled':'0.01','phase':'ACTIVE'})
    assert remaining_tp(c)==Decimal('0.01')
    assert len(active_tp_records(c))==1
    c['tpOrders'][-1]['filled']='0.02';c['tpOrders'][-1]['phase']='FILLED'
    assert remaining_tp(c)==0
    assert not active_tp_records(c)
    log('UNIT_TESTS_OK', rule='Virtual TP identity, partial fill, residual size')

def initial_state():
    pos=A.positions(); pending=A.pending()
    if any(D(p.get('pos'))!=0 for p in pos) or pending:
        die('Untracked ETH positions/orders. Refusing to initialize.')
    st={'version':VER,'instrument':INST,'cycles':{},'completed':0,'bootstrapped':True}
    save(st); return st

# Every historical SELL remains associated with its original BUY via cycleId.
# The exchange order ID changes on TP restoration, never the cycle identity.

def journal_tp(event, cycle, **extra):
    item={'time':datetime.now(timezone.utc).isoformat(),
          'event':event,'cycleId':cycle.get('cycleId'),
          'buyOrdId':cycle.get('ordId'),'buyPx':cycle.get('buyPx'), **extra}
    fd=os.open(TP_JOURNAL,os.O_WRONLY|os.O_CREAT|os.O_APPEND,0o600)
    with os.fdopen(fd,'a') as f:
        f.write(json.dumps(item,ensure_ascii=False,sort_keys=True)+'\n')
        f.flush();os.fsync(f.fileno())

def tp_records(c):
    return c.setdefault('tpOrders',[])

def cycle_sell_fill(c):
    return sum((D(y.get('filled')) for y in tp_records(c)),Decimal(0))

def remaining_tp(c):
    return D(c.get('filledBuy'))-cycle_sell_fill(c)

def active_tp_records(c):
    return [r for r in tp_records(c) if r['phase'] in
            ('PENDING','PLACING','ACTIVE','CANCELING')]

def virtual_cycles(st):
    return [(k,c) for k,c in st['cycles'].items() if
            c.get('state')=='VIRTUAL_TP' and remaining_tp(c)>0]

def migrate_cycle_records(st):
    changed=False
    for c in list(st.get('cycles',{}).values()) + st.get('archived_cycles',[]):
        if 'cycleId' not in c:
            if not c.get('ordId') and c.get('state') not in ('PLACING',):
                # A never-accepted BUY cannot be tracked as a verified cycle.
                c['cycleId']='pending-'+str(c.get('clOrdId') or secrets.token_hex(8))
            else:
                c['cycleId']='eth-'+str(c.get('ordId') or c.get('clOrdId'))
            changed=True
        if 'tpOrders' in c:continue
        orders=[]
        if c.get('tpClOrdId') and c.get('state') not in ('CANCELED',):
            if c.get('state')=='CLOSED': phase='FILLED'
            elif c.get('tpOrdId'):phase='ACTIVE'
            else: phase='PENDING'
            orders=[{'clOrdId':c['tpClOrdId'],'ordId':c.get('tpOrdId',''),
                     'sz':str(c.get('filledBuy') or c.get('sz') or QTY),
                     'filled':str(c.get('filledTP') or '0'),
                     'phase':phase,'source':'attached','placedAt':0}]
        c['tpOrders']=orders
        changed=True
    if changed:save(st)

def paged_order(cid, pages=12):
    """Exact client ID match; NEVER use price/quantity coincidence."""
    seen=set();after=None
    for _ in range(pages):
        params={'instType':'SWAP','instId':INST,'limit':'100'}
        if after:params['after']=after
        batch=A.req('GET','/api/v5/trade/orders-history',params)
        matches=[o for o in batch if o.get('side')=='sell' and o.get('clOrdId')==cid]
        if len(matches)>1:die('Ambiguous TP client ID: '+cid)
        if matches:return matches[0]
        if len(batch)<100:return None
        after=batch[-1].get('ordId')
        if not after or after in seen:die('History paging stalled')
        seen.add(after)
    return None  # Not enough information: do NOT assume cancellation.

def verify_existing(st,pending):
    known={c.get('ordId') for c in st['cycles'].values()}
    known_cls={c.get('clOrdId') for c in st['cycles'].values()}
    for c in st['cycles'].values():
        for rec in tp_records(c):
            known.add(rec.get('ordId'))
            known_cls.add(rec.get('clOrdId'))
    for o in pending:
        if o.get('ordId') not in known and o.get('clOrdId') not in known_cls:
            die('Unrecognized pending OKX order: '+str(o.get('ordId')))

def parent_tp_failure(c):
    parent=A.order(oid=c['ordId'])
    if not parent:die('Parent BUY not queryable: '+str(c['ordId']))
    if parent.get('state')!='filled':return ''
    for a in parent.get('attachAlgoOrds',[]):
        if a.get('attachAlgoClOrdId')==c.get('tpClOrdId'):
            return str(a.get('failCode') or '')
    return ''

def tp_record_update(st, c, rec, order):
    if order.get('instId')!=INST or order.get('side')!='sell':
        die('Wrong TP instrument/side '+c['cycleId'])
    if order.get('clOrdId')!=rec['clOrdId']:
        die('TP client identity mismatch '+c['cycleId'])
    if rec.get('ordId') and order.get('ordId')!=rec['ordId']:
        die('TP order ID mismatch '+c['cycleId'])
    if not order.get('ordId'):die('TP lacks ordId '+c['cycleId'])
    if D(order.get('sz'))!=D(rec['sz']):die('TP size changed externally '+c['cycleId'])
    if D(order.get('px'))!=D(c['tpPx']):die('TP price changed externally '+c['cycleId'])
    if order.get('reduceOnly') not in (None,'','true',True):
        die('TP not reduce-only: '+c['cycleId'])
    filled=D(order.get('accFillSz'))
    if filled<D(rec.get('filled')) or filled>D(rec['sz']):
        die('Nonmonotonic TP fill '+c['cycleId'])
    changed=False
    if rec.get('ordId')!=order['ordId']:
        rec['ordId']=order['ordId'];changed=True
        log('TP_LINKED',price=c['buyPx'],tpOrdId=order['ordId'],clOrdId=rec['clOrdId'])
    if filled!=D(rec.get('filled')):
        rec['filled']=str(filled);changed=True
    state=order.get('state')
    phases={'live':'ACTIVE','partially_filled':'ACTIVE',
            'filled':'FILLED','canceled':'CANCELED','mmp_canceled':'CANCELED'}
    if state not in phases:die('Unknown TP state '+str(state))
    new=phases[state]
    # A cancel ACK is asynchronous: until terminal canceled/filled, keep
    # CANCELING instead of treating an old 'live' snapshot as permission.
    if rec.get('phase')=='CANCELING' and new=='ACTIVE':
        # ACK can precede final exchange status by seconds during volatility.
        # Do not confuse an in-flight SELL cancel with an absent TP or retry
        # the same POST; keep cycle and remaining quantity reserved.
        elapsed=time.time()-float(rec.get('cancelAt') or time.time())
        if elapsed>=30 and time.time()-float(rec.get('cancelWarnAt',0))>=30:
            rec['cancelWarnAt']=time.time()
            changed=True
            log('TP_CANCEL_PENDING',price=c['buyPx'],ordId=rec['ordId'],seconds=int(elapsed))
        new='CANCELING'
    if rec.get('phase')!=new:
        rec['phase']=new;changed=True
        if new=='CANCELED':journal_tp('TP_CANCELED_CONFIRMED',c,ordId=rec['ordId'],filled=rec['filled'])
        if new=='FILLED':journal_tp('TP_FILLED_CONFIRMED',c,ordId=rec['ordId'],filled=rec['filled'])
    if changed:save(st)
    return new

def track(st,pending):
    """All TP replacements + fills are tracked under the original CycleID."""
    migrate_cycle_records(st)
    all_resolved=True
    byid={o.get('ordId'):o for o in pending}
    bycid={o.get('clOrdId'):o for o in pending}
    for key,c in list(st['cycles'].items()):
        status=c['state']
        if status in ('CLOSED','CANCELED'):continue
        if status=='PLACING':
            o=A.order(cid=c['clOrdId'])
            if not o:die('Uncertain BUY placement '+c['clOrdId'])
            c['ordId']=o['ordId'];c['state']='BUY_LIVE';save(st)
            status='BUY_LIVE'
        if status=='CANCELING':
            # POST cancel is only an ACK: exchange may still show live, or
            # this order may FILL while its cancel is travelling to OKX.
            # No new BUY submissions are allowed until the exact order ID
            # returns a final exchange state. This survives restarts.
            o=byid.get(c.get('ordId')) or A.order(oid=c['ordId'])
            if o is None:
                all_resolved=False
                continue
            if o.get('instId')!=INST or o.get('side')!='buy' or o.get('ordId')!=c.get('ordId'):
                die('BUY cancel identity mismatch '+key)
            state=o.get('state')
            filled=D(o.get('accFillSz'))
            if filled < D(c.get('filledBuy')) or filled > D(c['sz']):
                die('Nonmonotonic BUY cancel fill '+key)
            if state in ('canceled','mmp_canceled'):
                if filled:
                    # A partial canceled parent has no guaranteed attached TP.
                    # Preserve the filled quantity, stop trading rather than
                    # incorrectly freeing this grid or losing its exposure.
                    c['filledBuy']=str(filled)
                    c['state']='BUY_PARTIAL_CANCELLED'
                    save(st)
                    die('Partially filled BUY canceled without guaranteed TP at '+key+'; manual recovery required')
                c['state']='CANCELED';save(st)
                log('BUY_CANCEL_CONFIRMED',price=key,ordId=c['ordId'])
                continue
            if state=='filled':
                # Cancel lost the race. Keep SAME cycle ID, linked attached TP.
                c['state']='BUY_LIVE';save(st)
                log('BUY_FILLED_DURING_CANCEL',price=key,ordId=c['ordId'])
                status='BUY_LIVE'
            elif state in ('live','partially_filled'):
                if filled != D(c.get('filledBuy')):
                    c['filledBuy']=str(filled);save(st)
                all_resolved=False
                started=D(c.get('cancelAt'))
                elapsed=time.time()-float(started) if started else 0
                # Never blindly resubmit a cancellation or place replacement
                # buys. Log a warning periodically, but keep reconciling.
                if elapsed>=30 and time.time()-float(c.get('cancelWarnAt',0))>=30:
                    c['cancelWarnAt']=time.time();save(st)
                    log('BUY_CANCEL_PENDING',price=key,ordId=c['ordId'],
                        state=state,seconds=int(elapsed))
                continue
            else:
                die('Unknown BUY cancel state '+str(state)+' at '+key)
        if status in ('BUY_LIVE','BUY_PARTIAL'):
            o=byid.get(c.get('ordId')) or A.order(oid=c['ordId'])
            if not o:die('BUY unavailable '+str(c['ordId']))
            state=o.get('state');filled=D(o.get('accFillSz'))
            if state=='filled':
                c['state']='TP_WAIT';c['filledBuy']=str(filled)
                if not tp_records(c):
                    tp_records(c).append({'clOrdId':c['tpClOrdId'],
                        'ordId':c.get('tpOrdId',''),'sz':str(filled),'filled':'0',
                        'phase':'PENDING','source':'attached','placedAt':time.time()})
                save(st)
                log('BUY_FILLED',price=key,ordId=c['ordId'],tp=c['tpPx'])
            elif state=='partially_filled':
                c['state']='BUY_PARTIAL';c['filledBuy']=str(filled);save(st)
            elif state=='live':
                pass
            elif state in ('canceled','mmp_canceled'):
                if filled:die('BUY partially filled on cancel '+key)
                c['state']='CANCELED';save(st);continue
            else:die('Unknown BUY status '+str(state))
        if c['state'] not in ('TP_WAIT','TP_PARTIAL','VIRTUAL_TP'):
            continue
        for rec in tp_records(c):
            if rec['phase'] in ('FILLED','CANCELED','FAILED'):continue
            o=(byid.get(rec.get('ordId')) if rec.get('ordId') else None)
            if o is None:o=bycid.get(rec['clOrdId'])
            if o is None:
                o=A.order(oid=rec['ordId']) if rec.get('ordId') else A.order(cid=rec['clOrdId'])
            if o is None and rec['phase'] in ('PENDING','PLACING'):
                # Old 51165 TP failures are authoritative proof that no TP was placed.
                if rec.get('source')=='attached' and parent_tp_failure(c)=='51165':
                    rec['phase']='FAILED';rec['failCode']='51165';save(st)
                    journal_tp('ATTACHED_TP_FAIL_51165',c,clOrdId=rec['clOrdId'])
                    continue
            if o is None and rec['phase'] in ('PENDING','PLACING','CANCELING'):
                o=paged_order(rec['clOrdId'])
            if o is None:
                if rec['phase'] in ('ACTIVE','CANCELING'):
                    die('Previously known TP vanished without final state '+c['cycleId'])
                if rec.get('source')=='attached' and parent_tp_failure(c)=='51165':
                    rec['phase']='FAILED';rec['failCode']='51165';save(st)
                    continue
                if rec['phase']=='PLACING' and time.time()-rec.get('placedAt',0)>60:
                    die('TP POST unresolved after 60s '+c['cycleId'])
                all_resolved=False
                continue
            tp_record_update(st,c,rec,o)
        filled=cycle_sell_fill(c)
        bought=D(c.get('filledBuy'))
        if filled>bought:die('More TP fills than BUY '+c['cycleId'])
        if c.get('filledTP')!=str(filled):
            c['filledTP']=str(filled);save(st)
        active=active_tp_records(c)
        if filled==bought and bought>0:
            # Every TP leg must have terminated: no dangling reduce-only SELL.
            if active: die('Filled cycle still has live SELL '+c['cycleId'])
            if c['state']!='CLOSED':
                c['state']='CLOSED';st['completed']+=1;save(st)
                log('TP_FILLED',price=key,tpId=tp_records(c)[-1].get('ordId',''))
        elif active:
            if any(r['phase'] in ('PENDING','PLACING','CANCELING') for r in active):
                all_resolved=False
            target='TP_PARTIAL' if filled else 'TP_WAIT'
            if c['state']!=target:c['state']=target;save(st)
        else:
            if c['state']!='VIRTUAL_TP':
                c['state']='VIRTUAL_TP';save(st)
                log('TP_VIRTUAL',price=key,qty=str(bought-filled),cycleId=c['cycleId'])
    return all_resolved

def real_tp_active(st):
    return [(k,c,r) for k,c in st['cycles'].items()
            for r in tp_records(c) if r['phase']=='ACTIVE']

def coverage_audit(st,pending,position):
    active_sells=[o for o in pending if o.get('side')=='sell']
    total=sum((D(o.get('sz'))-D(o.get('accFillSz')) for o in active_sells),Decimal(0))
    virtual=sum((remaining_tp(c) for _,c in virtual_cycles(st)),Decimal(0))
    if total>position:die(f'Active TP quantity > position: {total}>{position}')
    if total+virtual!=position:
        die(f'Coverage mismatch: active {total}, virtual {virtual}, position {position}')
    if len(active_sells)>REDUCE_ONLY_HARD_CAP:
        die('Exchange TP count exceeds 150')
    if len(active_sells)!=len(real_tp_active(st)):
        die('Unmapped active TP count; no writes allowed')
    return len(active_sells),total,virtual


def cancel_tp(st,c,rec):
    """Durable TP cancellation intent. Only reconciliation may mark TP virtual."""
    if rec['phase']!='ACTIVE':die('Cancel requires known live TP')
    oid=rec['ordId']
    rec['phase']='CANCELING';rec['cancelAt']=time.time();save(st)
    journal_tp('CANCEL_INTENT',c,ordId=oid,clOrdId=rec['clOrdId'])
    res=A.req('POST','/api/v5/trade/cancel-order',body={'instId':INST,'ordId':oid})
    if len(res)!=1 or res[0].get('sCode')!='0':
        die('TP cancel rejected/ambiguous; intent is still recorded: '+str(res))
    journal_tp('CANCEL_ACCEPTED_PENDING_FINAL',c,ordId=oid)
    # The next reconciliation can observe live, canceled, partially filled,
    # or filled. No second POST, no premature release of this cycle.
    return 'CANCELING'


def place_virtual_tp(st,c):
    if c['state']!='VIRTUAL_TP' or active_tp_records(c):
        die('TP restore requires virtual-only cycle')
    remaining=remaining_tp(c)
    if remaining<=0:die('Cannot restore zero remaining TP')
    # Fractional size after a partial fill may be below OKX's lot step:
    # never round up and accidentally sell more than the open cycle.
    if remaining % QTY != 0:
        die('Virtual TP remainder below lot step; manual aggregation required: '+str(remaining))
    # Never exceed position coverage or exchange cap, even during surge.
    if len(real_tp_active(st))>=REAL_TP_TARGET:
        return False
    cid=newid('T')
    rec={'clOrdId':cid,'ordId':'','sz':str(remaining),'filled':'0',
         'phase':'PLACING','source':'recovery','placedAt':time.time()}
    tp_records(c).append(rec);c['state']='TP_WAIT';save(st)
    journal_tp('RESTORE_INTENT',c,clOrdId=cid,qty=str(remaining),px=c['tpPx'])
    body={'instId':INST,'tdMode':'cross','side':'sell','posSide':'net',
          'reduceOnly':True,'ordType':'limit','px':c['tpPx'],
          'sz':str(remaining),'clOrdId':cid}
    res=A.req('POST','/api/v5/trade/order',body=body)
    if len(res)!=1:die('Unknown TP restoration response '+c['cycleId'])
    if res[0].get('sCode')!='0':
        rec['phase']='FAILED';rec['failCode']=res[0].get('sCode');save(st)
        journal_tp('RESTORE_REJECTED',c,code=rec['failCode'],message=res[0].get('sMsg',''))
        die('TP restore rejected: '+str(res))
    if not res[0].get('ordId'):die('TP restore ACK missing ordId')
    rec['ordId']=res[0]['ordId'];save(st)
    journal_tp('RESTORE_ACCEPTED_PENDING_CONFIRMATION',c,ordId=rec['ordId'])
    # Query on the next reconcile, never assume acceptance equals live/fill.
    return True


def maintain_virtual_tp(st,pending,position):
    real,_,_=coverage_audit(st,pending,position)
    if real>REAL_TP_TARGET:
        # Preserve closest SELL first, remove highest-price TP first.
        farthest=sorted(real_tp_active(st),key=lambda y:D(y[1]['tpPx']),reverse=True)
        n=min(TP_ACTIONS_PER_PASS,real-REAL_TP_TARGET)
        for _,c,rec in farthest[:n]:cancel_tp(st,c,rec)
        return 'CANCEL'
    # At the target choose the nearest virtual TP only if it is closer
    # than the farthest currently live SELL. This avoids needless churn.
    virtual=sorted(virtual_cycles(st),key=lambda y:D(y[1]['tpPx']))
    if not virtual:return 'NONE'
    active=real_tp_active(st)
    if active and real>=REAL_TP_TARGET:
        farthest=max(active,key=lambda y:D(y[1]['tpPx']))
        if D(virtual[0][1]['tpPx'])<D(farthest[1]['tpPx']):
            cancel_tp(st,farthest[1],farthest[2]);return 'CANCEL'
        return 'NONE'
    if real<REAL_TP_TARGET:
        n=min(TP_ACTIONS_PER_PASS,REAL_TP_TARGET-real)
        for _,c in virtual[:n]:
            if len(real_tp_active(st))>=REAL_TP_TARGET:break
            place_virtual_tp(st,c)
        return 'RESTORE'
    return 'NONE'

def place(st,px):
    k=fmt(px); tp=fmt(px+TP)
    previous=st['cycles'].get(k)
    if previous:
        if previous['state'] not in ('CLOSED','CANCELED'):
            die('Level already occupied: '+k)
        st.setdefault('archived_cycles',[]).append({'level':k,**previous})
        save(st)
    cl=newid('B'); tcl=newid('T')
    x={'state':'PLACING','buyPx':k,'tpPx':tp,'sz':str(QTY),
       'cycleId':'eth-'+cl,'tpOrders':[],
       'clOrdId':cl,'tpClOrdId':tcl,'ordId':'','tpOrdId':''}
    st['cycles'][k]=x; save(st)  # durable intent BEFORE network request
    req={'instId':INST,'tdMode':'cross','side':'buy','posSide':'net',
         'ordType':'post_only','px':k,'sz':str(QTY),'clOrdId':cl,
         'attachAlgoOrds':[{'attachAlgoClOrdId':tcl,'tpOrdKind':'limit','tpOrdPx':tp}]}
    data=A.req('POST','/api/v5/trade/order',body=req)
    if len(data)!=1:
        die('Unexpected place-order response: '+str(data))
    if data[0].get('sCode') != '0':
        # Positive OKX rejection acknowledgement: no order was placed.
        x['state']='CANCELED'; x['rejectCode']=data[0].get('sCode'); save(st)
        log('BUY_REJECTED',price=k,code=x['rejectCode'],msg=data[0].get('sMsg',''))
        # Recalculate on next market snapshot; don't blindly resend here.
        return False
    if not data[0].get('ordId'):
        die('Order accepted without ID: manual check required')
    x['ordId']=data[0]['ordId']; x['state']='BUY_LIVE'; save(st)
    log('BUY_PLACED',price=k,tp=tp,ordId=x['ordId'])
    return True

def cancel_lowest_specific(st,lowest):
    if lowest.get('state')!='BUY_LIVE' or not lowest.get('ordId'):
        die('BUY cancellation requires confirmed live order')
    oid=lowest['ordId']
    # Write intent before network I/O. One POST only: on uncertain outcome,
    # CANCELING remains durable and reconciliation continues at next startup.
    lowest['state']='CANCELING'
    lowest['cancelAt']=time.time()
    save(st)
    data=A.req('POST','/api/v5/trade/cancel-order',body={'instId':INST,'ordId':oid})
    if len(data)!=1 or data[0].get('sCode')!='0':
        die('BUY cancel rejected/ambiguous; CANCELING persisted, reconcile before further writes: '+str(data))
    log('BUY_CANCEL_REQUESTED',cancelled=lowest['buyPx'],ordId=oid)
    # ACK is not a final state. No immediate GET or premature CANCELED here.
    # Next reconcile must observe canceled OR filled. In-between live means
    # WAIT, not STOP. On filled, attached TP remains bound to same CycleID.
    return True

class WSWatcher(threading.Thread):
    """Private order event stream; REST remains the authority for reconciliation."""
    def __init__(self):
        super().__init__(daemon=True)
        self.events=queue.Queue(maxsize=1000)
        self.wake=threading.Event()
        self.connected=False
    def run(self):
        while True:
            ws=None
            try:
                ws=websocket.create_connection('wss://ws.okx.com/ws/v5/private',
                    timeout=12,header=['User-Agent: curl/8.5.0'])
                ts=str(int(time.time()))
                msg=ts+'GET/users/self/verify'
                sig=base64.b64encode(hmac.new(ENV['OKX_API_SECRET'].encode(),
                    msg.encode(),hashlib.sha256).digest()).decode()
                ws.send(json.dumps({'op':'login','args':[{'apiKey':ENV['OKX_API_KEY'],
                    'passphrase':ENV['OKX_API_PASSPHRASE'],'timestamp':ts,'sign':sig}]}))
                ans=json.loads(ws.recv())
                if ans.get('event')!='login' or ans.get('code')!='0': die('WS login '+str(ans))
                ws.send(json.dumps({'op':'subscribe','args':[{'channel':'orders',
                    'instType':'SWAP','instId':INST}]}))
                ans=json.loads(ws.recv())
                if ans.get('event')!='subscribe': die('WS subscribe '+str(ans))
                self.connected=True
                ws.settimeout(5); last_ping=time.monotonic()
                while True:
                    if time.monotonic()-last_ping>=15:
                        ws.send('ping');last_ping=time.monotonic()
                    try: raw=ws.recv()
                    except websocket.WebSocketTimeoutException: continue
                    if raw=='ping': ws.send('pong');continue
                    if raw=='pong':continue
                    parsed=json.loads(raw)
                    if parsed.get('event')=='error': die('WS event error '+str(parsed))
                    if parsed.get('arg',{}).get('channel')=='orders':
                        for item in parsed.get('data',[]):
                            if item.get('instId')==INST:
                                safe={k:item.get(k) for k in ('ordId','clOrdId','algoClOrdId',
                                    'side','state','accFillSz','fillSz','tradeId')}
                                self.events.put_nowait(safe)
                                self.wake.set()
            except Exception as e:
                self.connected=False
                # Never log authentication messages containing credentials.
                print('[WS] reconnect in 5s:',type(e).__name__,flush=True)
                time.sleep(5)
            finally:
                self.connected=False
                if ws:
                    try: ws.close()
                    except Exception: pass


class PublicBidWatcher(threading.Thread):
    """Realtime OKX BID feed. Never sends private credentials.

    The ticker channel pushes a snapshot on price/best-bid changes (fastest
    published update period 100ms). REST is used if WS is disconnected or
    has not received an update for a while. A stale cached quote is NEVER used
    for placing/canceling live orders.
    """
    def __init__(self):
        super().__init__(daemon=True)
        self._lock=threading.Lock()
        self._bid=None
        self._updated=0.0
        self.connected=False
        self.wake=threading.Event()
        self.fallback_logged=False

    def run(self):
        while True:
            ws=None
            try:
                ws=websocket.create_connection('wss://ws.okx.com/ws/v5/public',
                    timeout=12,header=['User-Agent: curl/8.5.0'])
                ws.send(json.dumps({'op':'subscribe','args':[
                    {'channel':'tickers','instId':INST}]}))
                reply=json.loads(ws.recv())
                if reply.get('event') != 'subscribe':
                    die('Public BID subscription failed: '+str(reply))
                self.connected=True
                print('✅ Public BID WebSocket connected',flush=True)
                ws.settimeout(3)
                last_ping=time.monotonic()
                while True:
                    if time.monotonic()-last_ping >= 15:
                        ws.send('ping')
                        last_ping=time.monotonic()
                    try:
                        raw=ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    if raw=='ping': ws.send('pong'); continue
                    if raw=='pong': continue
                    msg=json.loads(raw)
                    if msg.get('event')=='error': die('Public BID WS error '+str(msg))
                    if msg.get('arg',{}).get('channel')!='tickers':
                        continue
                    for data in msg.get('data',[]):
                        if data.get('instId')!=INST: continue
                        bid=D(data.get('bidPx'))
                        if bid<=0: continue
                        with self._lock:
                            self._bid=bid
                            self._updated=time.monotonic()
                        self.wake.set()
            except Exception as e:
                print('[BID WS] reconnect in 5s:',type(e).__name__,flush=True)
                time.sleep(5)
            finally:
                self.connected=False
                with self._lock:
                    self._updated=0.0
                if ws:
                    try: ws.close()
                    except Exception: pass

    def get_bid(self):
        with self._lock:
            cached=self._bid
            age=time.monotonic()-self._updated
        if self.connected and cached is not None and age<=3.0:
            self.fallback_logged=False
            return cached
        # When an instrument stays unchanged the channel need not publish;
        # verify the quote with REST instead of assuming the old WS bid is fresh.
        if not self.fallback_logged:
            log('BID_REST_FALLBACK',reason='WS disconnected or quote >3s old')
            self.fallback_logged=True
        return A.ticker()


def grid_top(bid):
    top=(bid/STEP).to_integral_value(rounding=ROUND_FLOOR)*STEP
    if top>=bid: top-=STEP
    return top


def desired_buy_levels(top,occupied_tp):
    wanted=set()
    price=top
    while len(wanted)<TARGET and price>0:
        if price not in occupied_tp: wanted.add(price)
        price-=STEP
    if len(wanted)<TARGET: die('Insufficient valid grid levels')
    return wanted


class PnLWorker(threading.Thread):
    """Calculate real fill fees in background, NEVER delay trading on PnL I/O."""
    def __init__(self,pnl):
        super().__init__(daemon=True)
        self.pnl=pnl
        self.jobs=queue.Queue(maxsize=1)

    def submit(self,st):
        # Main thread alone owns/mutates trading state; the copy is immutable
        # to the worker and skips a file write if there are no closed cycles.
        if not any(x.get('state')=='CLOSED'
                   for x in list(st['cycles'].values())+st.get('archived_cycles',[])):
            return
        try: self.jobs.put_nowait(copy.deepcopy(st))
        except queue.Full: pass  # Next pass will submit a newer snapshot.

    def run(self):
        while True:
            state=self.jobs.get()
            try:
                self.pnl.work(state,max_cycles=2,historical=False)
            except Exception as e:
                print('⚠️ PnL background error:',type(e).__name__,str(e)[:140],flush=True)


def acquire_lock():
    path=ROOT/'state'/'bot05.lock'
    fd=os.open(path,os.O_CREAT|os.O_RDWR,0o600)
    try:
        fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError:
        os.close(fd)
        die('Another OKX bot05 instance is already running. STOP it first.')
    return fd


def ensure_state():
    if DATA.exists():
        st=json.loads(DATA.read_text())
        if st.get('instrument') != INST: die('Wrong state instrument')
        previous_version=st.get('version')
        if previous_version in ('0.4.1','0.5.0','0.5.1','0.5.2','0.5.3','0.5.4','0.5.5','0.5.6','0.5.7','0.5.8'):
            backup=DATA.with_name('bot04_state_before_v059.json')
            if not backup.exists():
                shutil.copy2(DATA,backup)
                os.chmod(backup,0o600)
            st['version']=VER
            st.setdefault('archived_cycles',[])
            save(st)
            log('STATE_UPGRADED',from_version=previous_version,to_version=VER,
                preserved_cycles=len(st['cycles']))
        elif previous_version != VER:
            die('Unsupported old state version: '+str(previous_version))
        migrate_cycle_records(st)
        return st
    if not LIVE:
        return {'cycles':{},'completed':0,'version':VER,'instrument':INST}
    return initial_state()


def reconcile_position(st, timeout=30, pause=1):
    """Read *fresh* order states and net position until they agree.

    Under rapid multiple fills a sequential GET /trade/order pass is not an
    atomic snapshot: later orders can fill AFTER we queried them, but BEFORE
    /account/positions. An immediate mismatch isn't proof of missing trades.
    NEVER submit new orders until a complete fresh pass reconciles. If the
    mismatch persists beyond the timeout, fail closed with existing orders
    still on the exchange.
    """
    started=time.monotonic()
    previous=None
    while True:
        pending=A.pending()
        resolved=track(st,pending)
        verify_existing(st,pending)
        positions=A.positions()
        expected=sum((D(x.get('filledBuy'))-D(x.get('filledTP'))
                      for x in st['cycles'].values()), Decimal('0'))
        actual=sum((D(p.get('pos')) for p in positions),Decimal('0'))
        if actual==expected:
            if previous is not None:
                log('POSITION_SYNC_OK',exchange=str(actual),local=str(expected),
                    retries=previous)
            return pending,resolved,actual
        remaining=timeout-(time.monotonic()-started)
        if remaining <= 0:
            die('Position mismatch persisted after full REST rechecks: '
                f'exchange={actual} local={expected}; no new BUY. '
                'Inspect pending orders and position before restarting.')
        # Expose the mismatch, rather than making a trading decision with
        # data from different moments in time.
        if previous is None:
            previous=0
        previous+=1
        if previous==1 or previous%5==0:
            log('POSITION_SYNC_WAIT',exchange=str(actual),local=str(expected),
                retry=previous,timeout_seconds=timeout)
        time.sleep(min(pause,max(0.1,remaining)))


def audit_once(st):
    pending,resolved,actual=reconcile_position(st,timeout=30)
    real,covered,virtual=coverage_audit(st,pending,actual)
    states={}
    for c in st['cycles'].values():
        states[c['state']]=states.get(c['state'],0)+1
    buys=sum(o.get('side')=='buy' for o in pending)
    print('=== TP VIRTUAL AUDIT v0.5.9 ===',flush=True)
    print('Position contracts:',actual,'| active TP contracts:',covered,
          '| VIRTUAL contracts:',virtual,flush=True)
    print('Open BUY:',buys,'| Real TP:',real,'| Virtual cycles:',len(virtual_cycles(st)),flush=True)
    print('Tracked states:',json.dumps(states,sort_keys=True),flush=True)
    print('CycleID unique:',len({c['cycleId'] for c in st['cycles'].values()})==len(st['cycles']))
    print('TP links resolved:',resolved,flush=True)
    if not resolved:
        die('Some TP identities unknown. No trading until fully reconciled.')
    print('✅ V059 AUDIT CONSISTENT — NO ORDERS SUBMITTED OR CANCELED',flush=True)
    print('⚠️ Virtual TP positions are UNPROTECTED while bot is offline.',flush=True)

def live_loop(st,watcher,market,pnl,pnl_worker):
    last_status_key=None
    last_print=0
    last_sync_notice=0.0
    last_sync_kind=None
    last_heartbeat=0.0
    while True:
        started=time.monotonic()
        if started-last_heartbeat>=15:
            runtime_heartbeat()
            last_heartbeat=started
        # Private WS acts as a wake signal; only REST-synchronized states may
        # cause order placement or rearming. Drop duplicate messages safely.
        while not watcher.events.empty():
            log('WS_ORDER',**watcher.events.get_nowait())
        watcher.wake.clear()

        pending,all_resolved,actual=reconcile_position(st,timeout=30)
        if all_resolved:
            pnl_worker.submit(st)
        if not all_resolved:
            buy_cancel_count=sum(c['state']=='CANCELING' for c in st['cycles'].values())
            tp_cancel_count=sum(r.get('phase')=='CANCELING'
                for c in st['cycles'].values() for r in tp_records(c))
            reason=(buy_cancel_count,tp_cancel_count)
            now=time.monotonic()
            if reason!=last_sync_kind or now-last_sync_notice>=10:
                if buy_cancel_count or tp_cancel_count:
                    print(f'⏳ Отмены на OKX ещё обрабатываются: BUY {buy_cancel_count}, '
                          f'TP {tp_cancel_count} | новые заявки на паузе',flush=True)
                else:
                    print('⏳ Проверяем привязку TP | новые BUY на паузе',flush=True)
                last_sync_notice=now;last_sync_kind=reason
            time.sleep(INTERVAL)
            continue

        # TP lifecycle takes precedence over all new BUY placements.
        # Any cancel/restore requires a full new REST reconciliation.
        bid=market.get_bid()
        tp_action=maintain_virtual_tp(st,pending,actual)
        if tp_action!='NONE':
            # No BUY until we have re-read the resulting SELL/position state.
            watcher.wake.wait(0.15);watcher.wake.clear()
            continue
        top=grid_top(bid)
        active=active_buy(st)
        if len(active)>TARGET: die('More than 25 active BUYs')

        # Catch up with a large upward jump *in this pass*, not one BUY every
        # 5+ seconds. Cancel at most 20 individually, confirming each cancel.
        # An order that fills while cancellation is pending causes fail-stop.
        moved=0
        if LIVE and len(active)>=TARGET:
            while moved < min(TARGET, 5):
                fresh_bid=market.get_bid()
                fresh_top=grid_top(fresh_bid)
                active=active_buy(st)
                if not active: break
                occupied_tp={D(k) for k,x in st['cycles'].items()
                    if x['state'] in ('TP_WAIT','TP_PARTIAL','VIRTUAL_TP')}
                desired=desired_buy_levels(fresh_top,occupied_tp)
                active_prices={D(x['buyPx']) for x in active}
                if not active_prices or fresh_top<max(active_prices): break
                obsolete=[x for x in active if D(x['buyPx']) not in desired
                          and x['state']=='BUY_LIVE']
                if not obsolete: break
                cancel_lowest_specific(st,min(obsolete,key=lambda x:D(x['buyPx'])))
                moved+=1
            if moved:
                # Cancelling is not a globally atomic snapshot. Reconcile all
                # fills/TPs before submitting replacement BUYs.
                pending,all_resolved,actual=reconcile_position(st,timeout=30)
                if not all_resolved:
                    print('WAIT_TP_SYNC after BUY rebalance',flush=True)
                    time.sleep(INTERVAL)
                    continue
                bid=market.get_bid()
                top=grid_top(bid)
                active=active_buy(st)

        # Hard reservation: every unfilled BUY may become one reduce-only TP.
        real_tp_count=len(real_tp_active(st))
        allowed_buys=max(0,min(TARGET,REDUCE_ONLY_HARD_CAP-real_tp_count-5))
        cap=max(0,min(allowed_buys-len(active),CAP-count(st),EX_CAP-len(pending)))
        candidates=[]
        price=top
        while len(candidates)<cap and price>0:
            if level_free(st,price): candidates.append(price)
            price-=STEP
        if not LIVE:
            print('DRY RUN', 'BID',bid,'current BUY',len(active),
                  'occupied',count(st),'would place',
                  [(fmt(p),fmt(p+TP)) for p in candidates],flush=True)
            break

        placed=0
        for price in candidates:
            if not level_free(st,price): continue
            if len(active_buy(st))>=allowed_buys or count(st)>=CAP: break
            # Under a fast fall the planned bid can become marketable.
            # POST_ONLY would reject it, but stop the stale batch earlier.
            if price>=market.get_bid():
                log('BUY_BATCH_PAUSED',reason='BID moved below planned level',
                    level=fmt(price))
                break
            if place(st,price): placed+=1
            else: break
            time.sleep(0.10)

        states={}
        for c in st['cycles'].values():
            states[c['state']]=states.get(c['state'],0)+1
        tps=states.get('TP_WAIT',0)+states.get('TP_PARTIAL',0)
        virtual=states.get('VIRTUAL_TP',0)
        status=(f'BID={bid} | BUY {len(active_buy(st))}/{TARGET} '
                f'| TP {tps} (реальных {len(real_tp_active(st))}) | виртуальных {virtual} | гриды {count(st)}/{CAP} '
                f'| закрыто {st["completed"]} | {pnl.status(st)}')
        now=time.monotonic()
        status_key=(len(active_buy(st)),tps,virtual,count(st),st['completed'],pnl.status(st))
        if status_key!=last_status_key or placed or moved or now-last_print>=60:
            print('📊 '+status,flush=True)
            last_status_key=status_key; last_print=now
        # Keep the safety loop at ~1s maximum idle delay. During REST work,
        # processing can still be slower; never promise a fixed 1s reaction.
        delay=max(0.0, INTERVAL-(time.monotonic()-started))
        if delay:
            watcher.wake.wait(delay)
            watcher.wake.clear()



# Full WEEX-style statistics, independent of the trading reconciliation thread.
# All numbers are derived from OKX's actual fills (not LIMIT prices).
STATS_INTERVAL = 600  # print immediately at start, then every ten minutes
STATS_HISTORY_PAGES_LIMIT = 200  # cap REST work and disclose if coverage fails
STATS_INTERVAL_BETWEEN_PAGES = 0.0  # Shared RestGovernor schedules fills-history calls


def stats_pct(num, denominator):
    if denominator <= 0:
        return '—'
    pct=num / denominator * Decimal(100)
    return f'{pct:.6f}%' if pct and abs(pct)<Decimal('0.01') else f'{pct:.4f}%' 


def stats_amount(x, digits=6):
    return f'${x:,.{digits}f}'


def full_stats_snapshot(state, fills, bid, contract_value):
    """Pure calculation over authoritative fill rows.

    Financial statistics are complete only if *every* expected BUY/TP fill
    for every tracked cycle is present. Partial history is NEVER represented
    as full lifetime accounting.
    """
    cycles=list(state.get('cycles',{}).values())+state.get('archived_cycles',[])
    grouped={}
    # idempotent even if API pagination boundaries overlap
    seen=set()
    unique_fills=[]
    for f in fills:
        oid=str(f.get('ordId') or '')
        tid=str(f.get('tradeId') or '')
        bill=str(f.get('billId') or '')
        if not oid or not tid or not bill: raise ValueError('Fill missing identifying fields')
        identity=(oid,bill)
        if identity in seen: continue
        seen.add(identity)
        unique_fills.append(f)
        if f.get('fee') in (None,''):
            raise ValueError('Missing actual fee; no estimate allowed')
        if f.get('feeCcy')!='USDT':
            raise ValueError('Non-USDT fee currency; cannot report exact USDT fees')
        if f.get('execType') not in ('M','T'):
            raise ValueError('Unknown Maker/Taker execution type')
        px=D(f['fillPx']); quantity=D(f['fillSz'])
        if px<=0 or quantity<=0: raise ValueError('Invalid fill size/price')
        g=grouped.setdefault(oid,{'qty':Decimal(0),'notional':Decimal(0),
                                   'fee':Decimal(0),'side':f.get('side')})
        if g['side'] != f.get('side'):
            raise ValueError('Mixed BUY/SELL under same ordId')
        g['qty']+=quantity
        g['notional']+=quantity*contract_value*px
        g['fee']+=D(f['fee'])

    closed_count=0
    closed_gain=Decimal(0)
    closed_buy_nominal=Decimal(0)
    open_gain=Decimal(0)
    open_buy_nominal=Decimal(0)
    expected={}
    known_orders=set()
    for c in cycles:
        buy_id=str(c.get('ordId') or '')
        sell_id=str(c.get('tpOrdId') or '')
        buy_sz=D(c.get('filledBuy'))
        sell_sz=D(c.get('filledTP'))
        if buy_sz:
            if not buy_id: raise ValueError('Missing BUY order ID for filled quantity')
            expected[buy_id]=buy_sz
            known_orders.add(buy_id)
        if sell_sz:
            legs=[r for r in c.get('tpOrders',[]) if D(r.get('filled'))>0]
            if not legs:legs=[{'ordId':sell_id,'filled':str(sell_sz)}]
            if sum((D(r['filled']) for r in legs),Decimal(0))!=sell_sz:
                raise ValueError('Cumulative TP quantity mismatch')
            for r in legs:
                oid=str(r.get('ordId') or '')
                if not oid:raise ValueError('TP leg without ordId')
                expected[oid]=D(r['filled'])
                known_orders.add(oid)

    missing=[(oid,qty,grouped.get(oid,{}).get('qty',Decimal(0)))
             for oid,qty in expected.items()
             if grouped.get(oid,{}).get('qty',Decimal(0)) != qty]
    if missing:
        return {'complete':False,'missing':missing[:4],
                'fill_orders_expected':len(expected),'fill_orders_seen':len(grouped)}

    maker_volume=taker_volume=maker_fee=taker_fee=Decimal(0)
    for f in unique_fills:
        oid=str(f.get('ordId') or '')
        if oid not in known_orders: continue
        px=D(f['fillPx']); qty=D(f['fillSz']); fee=D(f['fee'])
        if f['execType']=='M': maker_volume+=qty*contract_value*px;maker_fee+=fee
        elif f['execType']=='T': taker_volume+=qty*contract_value*px;taker_fee+=fee

    for c in cycles:
        b=grouped.get(str(c.get('ordId') or ''),None)
        legs=[r for r in c.get('tpOrders',[]) if D(r.get('filled'))>0]
        if not legs and c.get('tpOrdId'):
            legs=[{'ordId':c['tpOrdId'],'filled':c.get('filledTP','0')}]
        sell_parts=[grouped[str(r['ordId'])] for r in legs if str(r.get('ordId')) in grouped]
        if not b or b['qty']<=0: continue
        buy_avg=b['notional']/(b['qty']*contract_value)
        sell_qty=sum((v['qty'] for v in sell_parts),Decimal(0))
        if sell_qty>b['qty']: raise ValueError('TP exceeds BUY')
        sell_nominal=sum((v['notional'] for v in sell_parts),Decimal(0))
        sell_fee=sum((v['fee'] for v in sell_parts),Decimal(0))
        remaining=b['qty']-sell_qty
        if c.get('state')=='CLOSED':
            if remaining: raise ValueError('CLOSED cycle has unclosed position')
            closed_count+=1
            closed_gain += sell_nominal-b['notional']+b['fee']+sell_fee
            closed_buy_nominal+=b['notional']
        else:
            # Open-cycle mark-to-market, including realized portion of PARTIAL
            # TP and commissions already paid. Funding excluded.
            open_gain += (sell_nominal - sell_qty*contract_value*buy_avg +
                          remaining*contract_value*(bid-buy_avg) +
                          b['fee'] + sell_fee)
            open_buy_nominal+=b['notional']

    volume=maker_volume+taker_volume
    total_fees=maker_fee+taker_fee
    total_gain=closed_gain+open_gain
    total_buy=closed_buy_nominal+open_buy_nominal
    active_buys=sum(c.get('state') in ('BUY_LIVE','BUY_PARTIAL','PLACING') for c in state.get('cycles',{}).values())
    tps=sum(c.get('state') in ('TP_WAIT','TP_PARTIAL') for c in state.get('cycles',{}).values())
    open_contracts=sum((D(c.get('filledBuy'))-D(c.get('filledTP'))
                        for c in state.get('cycles',{}).values()),Decimal(0))
    return dict(complete=True,bid=bid,closed_count=closed_count,
                active_buys=active_buys,tps=tps,grid_gain=closed_gain,
                grid_pct=stats_pct(closed_gain,closed_buy_nominal),
                open_gain=open_gain,open_pct=stats_pct(open_gain,open_buy_nominal),
                total_gain=total_gain,total_pct=stats_pct(total_gain,total_buy),
                total_fees=total_fees,volume=volume,maker_volume=maker_volume,
                taker_volume=taker_volume,maker_fee=maker_fee,taker_fee=taker_fee,
                maker_percent=stats_pct(maker_volume,volume),
                taker_percent=stats_pct(taker_volume,volume),
                maker_fee_pct=stats_pct(-maker_fee,maker_volume),
                taker_fee_pct=stats_pct(-taker_fee,taker_volume),
                open_contracts=open_contracts,open_eth=open_contracts*contract_value,
                account_closed=state.get('completed',0),
                expected_orders=len(expected))


def stats_exchange_fills(state):
    cycles=list(state.get('cycles',{}).values())+state.get('archived_cycles',[])
    expected={}
    for c in cycles:
        if D(c.get('filledBuy'))>0 and c.get('ordId'):
            expected[str(c['ordId'])]=D(c['filledBuy'])
        if D(c.get('filledTP'))>0:
            legs=[r for r in c.get('tpOrders',[]) if D(r.get('filled'))>0]
            if not legs:legs=[{'ordId':c.get('tpOrdId'),'filled':c.get('filledTP')}]
            for r in legs:
                if r.get('ordId'):expected[str(r['ordId'])]=D(r['filled'])
    if not expected:
        return []
    got={}
    rows=[]; seen_bills=set(); after=None
    for page_number in range(STATS_HISTORY_PAGES_LIMIT):
        params={'instType':'SWAP','instId':INST,'limit':'100'}
        if after: params['after']=after
        page=A.req('GET','/api/v5/trade/fills-history',params,background=True)
        for f in page:
            oid=str(f.get('ordId') or '')
            bill=str(f.get('billId') or '')
            if oid not in expected or not bill or bill in seen_bills:continue
            seen_bills.add(bill)
            got[oid]=got.get(oid,Decimal(0))+D(f.get('fillSz'))
            rows.append(f)
        if all(got.get(oid,Decimal(0))==q for oid,q in expected.items()):
            return rows
        if len(page)<100: return rows
        nxt=str(page[-1].get('billId') or '')
        if not nxt or nxt==after: raise RuntimeError('Fills pagination stalled')
        after=nxt
    raise RuntimeError('Too many historical pages; stats coverage not verified')


# WEEX-style report: fixed capital basis from the FIRST accepted BUY price.
# Revenue/fees/execution remain actual OKX fills, not LIMIT-price estimates.
RUNTIME_STATS = ROOT / 'state' / 'okx_stats_runtime_v059.json'
GRID_REFERENCE = ROOT / 'state' / 'okx_stats_grid_reference_v059.json'

def _first_anchor(fills):
    # Freeze the original 1x-capital reference, so log rotation or movement
    # of the dynamic BUY grid cannot silently change historical % return.
    if GRID_REFERENCE.exists():
        r=json.loads(GRID_REFERENCE.read_text())
        return float(r['started_at']),D(r['top_buy'])
    # Prefer persistent order-placement events, which predate fill data.
    oldest_start = None
    oldest_buy = None
    if EVENTS.exists():
        with EVENTS.open(encoding='utf-8') as handle:
            for line in handle:
                try:
                    ev=json.loads(line)
                    dt=datetime.fromisoformat(ev['ts'].replace('Z','+00:00'))
                    if dt.tzinfo is None: continue
                    stamp=dt.astimezone(timezone.utc).timestamp()
                    if ev.get('event')=='START' and ev.get('live') is True:
                        oldest_start=min(oldest_start,stamp) if oldest_start else stamp
                    if ev.get('event')=='BUY_PLACED' and ev.get('price'):
                        x=(stamp,D(ev['price']))
                        if oldest_buy is None or x[0]<oldest_buy[0]: oldest_buy=x
                except (ValueError,KeyError,TypeError,json.JSONDecodeError):
                    continue
    first_fill=None
    for f in fills:
        ts=f.get('ts') or f.get('fillTime')
        if ts and str(ts).isdigit():
            t=int(ts)/1000
            first_fill=min(first_fill,t) if first_fill is not None else t
    launched=oldest_start or (oldest_buy[0] if oldest_buy else first_fill)
    top=oldest_buy[1] if oldest_buy else None
    if launched is not None and top is not None:
        if top<=0 or top%STEP:
            raise ValueError('Invalid initial grid price; cannot fix 1x reference')
        atomic_json(GRID_REFERENCE,{'started_at':launched,'top_buy':str(top),
                                    'grid_step':str(STEP),'max_grids':CAP})
    return launched,top


def _clock(seconds):
    seconds=max(0,int(seconds))
    days,rem=divmod(seconds,86400)
    hours,rem=divmod(rem,3600)
    minutes,secs=divmod(rem,60)
    return (f'{days}д {hours}ч {minutes}м {secs}с' if days else
            f'{hours}ч {minutes}м {secs}с')


def runtime_heartbeat():
    # Accurate uptime tracking begins ONLY with v0.5.9.
    # Gaps >60s are downtime, not falsely counted as running time.
    now=time.time()
    if RUNTIME_STATS.exists():
        d=json.loads(RUNTIME_STATS.read_text())
    else:
        d={'since':now,'last':now,'seconds':0.0}
    delta=now-float(d.get('last',now))
    if 0 <= delta <= 60:
        d['seconds']=float(d.get('seconds',0))+delta
    d['last']=now
    atomic_json(RUNTIME_STATS,d)


def runtime_pct(now):
    if not RUNTIME_STATS.exists(): return '—'
    d=json.loads(RUNTIME_STATS.read_text())
    elapsed=max(0,now-float(d['since']))
    if elapsed<60: return '—'
    running=max(0,float(d.get('seconds',0)))
    return f'{min(100.,100.*running/elapsed):.2f}%'


def _last_24h_profit(st,fills, contract_value, now_ms):
    """Sum completed-cycle net after actual fills, by LAST SELL fill time."""
    grouped={}
    seen=set()
    for f in fills:
        oid=str(f.get('ordId') or '')
        bill=str(f.get('billId') or '')
        if not oid or not bill: raise ValueError('Missing fill identity')
        k=(oid,bill)
        if k in seen:continue
        seen.add(k)
        ts=f.get('ts') or f.get('fillTime')
        if not ts or not str(ts).isdigit():
            raise ValueError('OKX fills missing timestamp for 24h profit')
        row=grouped.setdefault(oid,[])
        row.append((int(ts),D(f['fillSz'])*contract_value*D(f['fillPx']),D(f['fee'])))
    cutoff=now_ms-86400000
    total=Decimal(0)
    for c in list(st.get('cycles',{}).values())+st.get('archived_cycles',[]):
        if c.get('state')!='CLOSED': continue
        bidid=str(c.get('ordId') or '')
        buys=grouped.get(bidid,[])
        legs=[r for r in c.get('tpOrders',[]) if D(r.get('filled'))>0]
        if not legs and c.get('tpOrdId'):
            legs=[{'ordId':c['tpOrdId'],'filled':c.get('filledTP','0')}]
        sells=[x for r in legs for x in grouped.get(str(r.get('ordId')),[])]
        if not buys or not sells: raise ValueError('Closed-cycle fills missing')
        if max(t for t,_,_ in sells)>=cutoff:
            total += (sum((n+fee for _,n,fee in sells),Decimal(0))
                      - sum((n-fee for _,n,fee in buys),Decimal(0)))
    return total


def format_weex_report(st,fills,stats,now=None):
    now = time.time() if now is None else float(now)
    launch,top=_first_anchor(fills)
    elapsed=now-launch if launch is not None else None
    # The grid is the original reference range, NOT the current count of
    # virtual/real TP. Same 1x capital logic as WEEX (capacity x avg grid).
    if top is not None:
        bottom=top-STEP*(CAP-1)
        if bottom<=0: raise ValueError('Invalid reference-grid lower bound')
        average=(top+bottom)/2
        max_eth=CAP*QTY*CONTRACT_VALUE
        capital=max_eth*average
    else:
        bottom=average=max_eth=capital=None
    if capital is not None:
        past24=_last_24h_profit(st,fills,CONTRACT_VALUE,int(now*1000))
    else:
        past24=None
    days=Decimal(str(elapsed))/Decimal('86400') if elapsed and elapsed>0 else None
    perday=stats['grid_gain']/days if days else None
    per30=perday*30 if perday is not None else None
    peryear=perday*365 if perday is not None else None
    rate=lambda v: stats_pct(v,capital) if v is not None and capital else '—'
    raw=lambda v,p=4: f'${v:,.{p}f}' if v is not None else '—'
    plus=lambda v,p=4: money_usdt(v,p) if v is not None else '—'
    datestr=(datetime.fromtimestamp(launch,timezone.utc).strftime('%d.%m.%Y %H:%M:%S UTC')
             if launch is not None else '—')
    nowstr=datetime.fromtimestamp(now,timezone.utc).strftime('%H:%M:%S UTC')
    real=len(real_tp_active(st))
    virtual=len(virtual_cycles(st))
    items=[
        f'[{nowstr}] 📊 OKX ETH GRID STATISTICS',
        '============================================',
        f'Запущен:                   {datestr}',
        f'Работает:                  {_clock(elapsed) if elapsed is not None else "—"}',
        f'Uptime:                    {runtime_pct(now)}',
        f'Текущий BID:               {raw(stats["bid"],2)}',
        f'Закрытых циклов:           {stats["closed_count"]}',
        f'Открыто ордеров:           {stats["active_buys"]+real}',
        f'  BUY LIMIT:               {stats["active_buys"]}',
        f'  ждут TP:                 {real}',
        f'  виртуальных TP:          {virtual}',
        f'GRID PROFIT:               {plus(stats["grid_gain"],8)}',
        f'Прибыль за последние 24ч: {plus(past24)}   ({rate(past24)})',
        'Средний темп:',
        f'  сутки:                   {plus(perday)}   ({rate(perday)})',
        f'  30 дней:                 {plus(per30,2)}   ({rate(per30)})',
        f'  год:                     {plus(peryear,2)}   ({rate(peryear)})',
        f'Расчётный капитал для 1x: {raw(capital,2) if capital is not None else "—"}',
        f'  max position:            {max_eth:.4f} ETH' if max_eth is not None else '  max position:            —',
        f'  сетка:                   {raw(top,2)} → {raw(bottom,2)}' if top is not None else '  сетка:                   —',
        f'  средняя цена:            {raw(average,2)}' if average is not None else '  средняя цена:            —',
        f'Уплачено комиссий:        {raw(-stats["total_fees"],8)}',
        f'Общий объём:              {raw(stats["volume"],4)}',
        f'  Maker:                   {raw(stats["maker_volume"],4)}',
        f'  Taker:                   {raw(stats["taker_volume"],4)}',
        f'Открытая позиция:         {stats["open_eth"]:.4f} ETH',
        f'Нереализованный PnL:      {plus(stats["open_gain"],8)}',
        '--------------------------------------------',
        f'ИТОГОВЫЙ PnL:             {plus(stats["total_gain"],8)} ({rate(stats["total_gain"])})',
        '--------------------------------------------',
        '✅ Все закрытые циклы сопоставлены.',
    ]
    return '\n'.join(items)


def print_full_stats(pnl=None):
    """One-shot report / 10-minute background print; NEVER touches orders."""
    if not DATA.exists():
        print('📊 GRID STATISTICS: торговое состояние ещё не создано',flush=True)
        return
    st=json.loads(DATA.read_text())
    if st.get('instrument')!=INST:
        raise ValueError('Statistics file is not for ETH-USDT-SWAP')
    bid=A.ticker()
    fills=stats_exchange_fills(st)
    d=full_stats_snapshot(st,fills,bid,CONTRACT_VALUE)
    if d.get('complete') and pnl is not None:
        recovered=pnl.ingest_history(st,fills)
        if recovered:
            print(f'✅ Финансовый журнал дополнен: {recovered} закрытых TP',flush=True)
        ledger_total=pnl.total()
        if abs(ledger_total-d['grid_gain'])>Decimal('0.00000001'):
            print(f'⚠️ Расхождение PnL журнала и отчёта: {ledger_total} / {d["grid_gain"]}',flush=True)
    if not d['complete']:
        print('⚠️ Статистика неполная: часть исполнений отсутствует',flush=True)
        print('Исполнения для ордеров:',d['fill_orders_seen'],'/',d['fill_orders_expected'],flush=True)
        print('Примеры расхождений:',d['missing'],flush=True)
        print('Финансовые суммы не выводятся.',flush=True)
        return
    print('\n'+format_weex_report(st,fills,d)+'\n',flush=True)


class StatsWorker(threading.Thread):
    """Independent read-only stats. Never blocks or modifies order management."""
    def __init__(self,pnl):
        super().__init__(daemon=True)
        self.pnl=pnl

    def run(self):
        while True:
            delay=STATS_INTERVAL
            try:
                print_full_stats(self.pnl)
            except RateLimitedError:
                delay=60
                print('⏳ OKX ограничил историю сделок; статистика повторится через 60с',flush=True)
            except Exception as e:
                print('⚠️ Статистика недоступна:',type(e).__name__,str(e)[:180],flush=True)
            time.sleep(delay)


def main():
    if STATS_ONLY:
        print_full_stats()  # read-only, no lock, safe while LIVE bot runs
        return
    lock=acquire_lock()
    try:
        print(f'=== OKX BOT01 v{VER} ===',
              'LIVE 25 BUY' if LIVE else ('READ ONLY AUDIT' if AUDIT else 'READ ONLY'),flush=True)
        run_tests()
        a=A.req('GET','/api/v5/account/config')
        if not a or a[0].get('posMode')!='net_mode': die('Only net_mode supported')
        info=A.instrument()
        value=D(info.get('ctVal'))
        if info.get('ctValCcy')!='ETH' or value!=CONTRACT_VALUE:
            die('Unexpected ETH contract value. STOP for safe PnL calculation.')
        st=ensure_state()
        pnl=PnLLedger(value)
        pnl.initial_ids={pnl.identity(c) for c in pnl.all_cycles(st)}
        log('START',live=LIVE,audit=AUDIT,tracked=count(st),
            realized_usdt=str(pnl.total()))
        if AUDIT:
            audit_once(st)
            pnl.work(st,max_cycles=1,historical=True)
            print('💰',pnl.status(st),flush=True)
            try: print_full_stats()
            except Exception as e:
                print('⚠️ Статистика недоступна:',type(e).__name__,str(e)[:180],flush=True)
            return
        watcher=WSWatcher();watcher.start()
        market=PublicBidWatcher();market.start()
        pnl_worker=PnLWorker(pnl);pnl_worker.start()
        stats_worker=StatsWorker(pnl);stats_worker.start()
        live_loop(st,watcher,market,pnl,pnl_worker)
    finally:
        os.close(lock)

if __name__=='__main__':
    try:main()
    except KeyboardInterrupt: print('\nStopped (open orders remain on OKX).',flush=True)
    except Exception as exc:
        print('⛔ STOP:',str(exc),file=sys.stderr,flush=True)
        sys.exit(1)
