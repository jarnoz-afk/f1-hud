#!/usr/bin/env python3
"""
F1 HUD live-server.

Leest de gratis live-timingfeed van F1 (SignalR Core, zonder inlog) en biedt
de data aan in hetzelfde formaat als OpenF1, zodat de HUD er live op kan draaien.
Serveert ook de HUD zelf (index.html in dezelfde map).

Gebruik:
    pip install websockets
    python f1hud_server.py
Open daarna op je telefoon het adres dat in beeld verschijnt (zelfde wifi).

Zonder F1 TV-account geeft de feed geen GPS-posities en geen telemetrie.
De kaart toont dan het circuit, maar geen auto's.
"""
import asyncio
import json
import os
import re
import socket
import threading
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import unquote, urlsplit

try:
    import websockets
except ImportError:
    raise SystemExit('Installeer eerst websockets:  pip install websockets')

PORT = int(os.environ.get('F1HUD_PORT', '8765'))
NEGOTIATE_URL = 'https://livetiming.formula1.com/signalrcore/negotiate'
WS_URL = 'wss://livetiming.formula1.com/signalrcore'
STATIC_URL = 'https://livetiming.formula1.com/static/'
RS = '\x1e'
TOPICS = ['SessionInfo', 'Heartbeat', 'SessionStatus', 'SessionData', 'TrackStatus', 'DriverList',
          'TimingData', 'TimingAppData', 'TimingStats', 'WeatherData', 'RaceControlMessages',
          'ExtrapolatedClock', 'LapCount', 'TeamRadio']
HERE = os.path.dirname(os.path.abspath(__file__))


# ---------------------------------------------------------------- hulpfuncties
def utc(s):
    """ISO-tekst naar datetime in UTC. Tekst zonder tijdzone wordt als UTC gelezen."""
    if not s:
        return None
    s = str(s).strip().replace('Z', '+00:00')
    m = re.match(r'^(.*\.\d{6})\d+(.*)$', s)       # F1 stuurt soms 7 decimalen
    if m:
        s = m.group(1) + m.group(2)
    try:
        d = datetime.fromisoformat(s)
    except ValueError:
        return None
    return d.replace(tzinfo=timezone.utc) if d.tzinfo is None else d.astimezone(timezone.utc)


def iso(d):
    return d.astimezone(timezone.utc).isoformat()


def as_list(v):
    if isinstance(v, list):
        return v
    if isinstance(v, dict):
        items = []
        for k, x in v.items():
            try:
                items.append((int(k), x))
            except ValueError:
                pass
        return [x for _, x in sorted(items)]
    return []


def lap_seconds(v):
    """'1:35.123' of '35.123' naar seconden."""
    if not v:
        return None
    try:
        if ':' in v:
            m, s = v.split(':', 1)
            return round(int(m) * 60 + float(s), 3)
        return round(float(v), 3)
    except ValueError:
        return None


def gap_value(v):
    """'+1.234' -> 1.234, '1L' / '2 L' -> '+1 LAP', 'LAP 23' (leider) -> None."""
    if v is None or v == '':
        return None
    v = str(v).strip()
    if v.upper().startswith('LAP'):
        return None
    m = re.match(r'^\+?(\d+)\s*L', v, re.I)
    if m:
        n = int(m.group(1))
        return f'+{n} LAP' + ('S' if n > 1 else '')
    try:
        return round(float(v.lstrip('+')), 3)
    except ValueError:
        return v


def merge(base, upd):
    """Delta van de feed samenvoegen met de bestaande toestand."""
    if isinstance(upd, dict):
        if isinstance(base, list):
            out = list(base)
            for k, v in upd.items():
                try:
                    i = int(k)
                except ValueError:
                    continue
                while len(out) <= i:
                    out.append({})
                out[i] = merge(out[i], v)
            return out
        out = dict(base) if isinstance(base, dict) else {}
        for k, v in upd.items():
            if k == '_deleted':
                for d in (v if isinstance(v, list) else [v]):
                    out.pop(str(d), None)
                continue
            out[k] = merge(out.get(k), v)
        return out
    return upd


# ---------------------------------------------------------------- toestand
class Live:
    def __init__(self):
        self.lock = threading.Lock()
        self.connected = False
        self.last_msg = 0.0
        self.reset()

    def reset(self):
        self.state = {}
        self.session = None
        self.total_laps = None
        self.rows = {k: [] for k in ('position', 'intervals', 'pit', 'race_control', 'weather', 'team_radio')}
        self.laps = {}
        self.stints = {}
        self.drv = {}
        self.rc_seen = set()
        self.radio_seen = set()

    # --- opslag
    def keys(self):
        s = self.session or {}
        return {'session_key': s.get('session_key'), 'meeting_key': s.get('meeting_key')}

    def add(self, kind, rec):
        rec.update(self.keys())
        self.rows[kind].append(rec)

    def lap(self, n, k):
        key = (n, k)
        if key not in self.laps:
            self.laps[key] = {'driver_number': n, 'lap_number': k, 'date_start': None, 'lap_duration': None,
                              'duration_sector_1': None, 'duration_sector_2': None, 'duration_sector_3': None,
                              'st_speed': None, 'is_pit_out_lap': False, **self.keys()}
        return self.laps[key]

    # --- verwerken
    def handle(self, topic, data, ts, keyframe):
        if isinstance(data, str):
            try:
                data = json.loads(data)
            except ValueError:
                return
        with self.lock:
            self.last_msg = time.time()
            full = data if keyframe else merge(self.state.get(topic), data)
            self.state[topic] = full
            fn = getattr(self, 'on_' + topic, None)
            if fn and isinstance(full, dict):
                fn(data if isinstance(data, dict) else {}, full, ts, keyframe)

    def on_SessionInfo(self, d, f, ts, kf):
        key = f.get('Key')
        if self.session and key and key != self.session['session_key']:
            print(f'Nieuwe sessie: {f.get("Name")}. Oude data gewist.')
            keep = self.state
            self.reset()
            self.state = {'SessionInfo': keep.get('SessionInfo')}
        meet = f.get('Meeting') or {}
        off = f.get('GmtOffset') or '00:00:00'
        neg = off.startswith('-')
        h, m, *_ = (off.lstrip('-+').split(':') + ['0', '0'])
        delta = timedelta(hours=int(h), minutes=int(m)) * (-1 if neg else 1)
        def to_utc(v):
            x = utc(v)
            return iso(x - delta) if x else None
        start = to_utc(f.get('StartDate'))
        self.session = {
            'session_key': key, 'meeting_key': meet.get('Key'),
            'session_name': f.get('Name'), 'session_type': f.get('Type'),
            'date_start': start, 'date_end': to_utc(f.get('EndDate')),
            'location': meet.get('Location'), 'country_name': (meet.get('Country') or {}).get('Name'),
            'circuit_short_name': (meet.get('Circuit') or {}).get('ShortName'),
            'year': int(start[:4]) if start else None, 'path': f.get('Path'),
            'total_laps': self.total_laps,
        }

    def on_LapCount(self, d, f, ts, kf):
        if f.get('TotalLaps'):
            self.total_laps = int(f['TotalLaps'])
            if self.session:
                self.session['total_laps'] = self.total_laps

    def drivers(self):
        out = []
        for k, v in (self.state.get('DriverList') or {}).items():
            if not isinstance(v, dict) or not k.isdigit():
                continue
            out.append({'driver_number': int(v.get('RacingNumber') or k), 'broadcast_name': v.get('BroadcastName'),
                        'full_name': v.get('FullName'), 'name_acronym': v.get('Tla'), 'team_name': v.get('TeamName'),
                        'team_colour': v.get('TeamColour'), 'first_name': v.get('FirstName'),
                        'last_name': v.get('LastName'), 'headshot_url': v.get('HeadshotUrl'), **self.keys()})
        return out

    def on_TimingData(self, d, f, ts, kf):
        lines_full = f.get('Lines') or {}
        for num, dl in (d.get('Lines') or {}).items():
            if not isinstance(dl, dict) or not num.isdigit():
                continue
            L = lines_full.get(num) or {}
            n = int(num)
            st = self.drv.setdefault(n, {'nl': None, 'inpit': False, 'pit': None})
            nl = int(L.get('NumberOfLaps') or 0)
            if st['nl'] is None or kf:
                st['nl'] = nl
            if 'Position' in dl and L.get('Position'):
                self.add('position', {'date': iso(ts), 'driver_number': n, 'position': int(L['Position'])})
            if 'GapToLeader' in dl or 'IntervalToPositionAhead' in dl:
                self.add('intervals', {'date': iso(ts), 'driver_number': n,
                                       'gap_to_leader': gap_value(L.get('GapToLeader')),
                                       'interval': gap_value((L.get('IntervalToPositionAhead') or {}).get('Value'))})
            if 'NumberOfLaps' in dl and not kf and nl > st['nl']:
                self.lap(n, nl + 1)['date_start'] = iso(ts)     # ronde nl klaar, ronde nl+1 begint nu
                st['nl'] = nl
            if 'LastLapTime' in dl and nl:
                v = lap_seconds((L.get('LastLapTime') or {}).get('Value'))
                if v:
                    self.lap(n, nl)['lap_duration'] = v
            if 'Sectors' in dl:
                secs = as_list(L.get('Sectors'))
                changed = dl['Sectors'].keys() if isinstance(dl['Sectors'], dict) else range(len(dl['Sectors']))
                for i in changed:
                    try:
                        i = int(i)
                        v = lap_seconds((secs[i] or {}).get('Value'))
                    except (ValueError, IndexError, AttributeError):
                        continue
                    if not v:
                        continue
                    target = nl if (i == 2 and 'NumberOfLaps' in dl) else nl + 1
                    if target >= 1:
                        self.lap(n, target)[f'duration_sector_{i + 1}'] = v
            st_speed = ((dl.get('Speeds') or {}).get('ST') or {})
            if st_speed:
                try:
                    v = int(((L.get('Speeds') or {}).get('ST') or {}).get('Value'))
                    self.lap(n, nl + 1)['st_speed'] = v
                except (TypeError, ValueError):
                    pass
            if 'InPit' in dl and not kf:
                inp = bool(L.get('InPit'))
                if inp and not st['inpit']:
                    rec = {'date': iso(ts), 'driver_number': n, 'lap_number': nl + 1,
                           'lane_duration': None, 'pit_duration': None, 'stop_duration': None}
                    self.add('pit', rec)
                    st['pit'] = rec
                elif not inp and st['inpit'] and st['pit']:
                    dur = round((ts - utc(st['pit']['date'])).total_seconds(), 1)
                    st['pit']['lane_duration'] = st['pit']['pit_duration'] = dur
                st['inpit'] = inp
            if 'PitOut' in dl and L.get('PitOut') and not kf:
                self.lap(n, nl + 1)['is_pit_out_lap'] = True

    def on_TimingAppData(self, d, f, ts, kf):
        for num, L in (f.get('Lines') or {}).items():
            if not isinstance(L, dict) or not num.isdigit():
                continue
            n, start, out = int(num), 1, []
            stints = as_list(L.get('Stints'))
            for i, s in enumerate(stints):
                if not isinstance(s, dict):
                    continue
                total, age = int(s.get('TotalLaps') or 0), int(s.get('StartLaps') or 0)
                last = i == len(stints) - 1
                end = None if last else start + max(0, total - age) - 1
                out.append({'driver_number': n, 'stint_number': i + 1, 'compound': s.get('Compound'),
                            'tyre_age_at_start': age, 'lap_start': start, 'lap_end': end, **self.keys()})
                if end is not None:
                    start = end + 1
            self.stints[n] = out

    def on_RaceControlMessages(self, d, f, ts, kf):
        for m in as_list(f.get('Messages')):
            if not isinstance(m, dict):
                continue
            key = (m.get('Utc'), m.get('Message'))
            if key in self.rc_seen:
                continue
            self.rc_seen.add(key)
            when = utc(m.get('Utc')) or ts
            num = m.get('RacingNumber')
            self.add('race_control', {'date': iso(when), 'category': m.get('Category'), 'flag': m.get('Flag'),
                                      'scope': m.get('Scope'), 'sector': m.get('Sector'), 'message': m.get('Message'),
                                      'lap_number': m.get('Lap'),
                                      'driver_number': int(num) if str(num or '').isdigit() else None})
        self.rows['race_control'].sort(key=lambda r: r['date'])

    def on_TeamRadio(self, d, f, ts, kf):
        path = (self.session or {}).get('path') or ''
        for c in as_list(f.get('Captures')):
            if not isinstance(c, dict) or c.get('Path') in self.radio_seen:
                continue
            self.radio_seen.add(c.get('Path'))
            when = utc(c.get('Utc')) or ts
            num = c.get('RacingNumber')
            self.add('team_radio', {'date': iso(when), 'driver_number': int(num) if str(num or '').isdigit() else None,
                                    'recording_url': STATIC_URL + path + (c.get('Path') or '')})
        self.rows['team_radio'].sort(key=lambda r: r['date'])

    def on_WeatherData(self, d, f, ts, kf):
        def num(k):
            try:
                return float(f.get(k))
            except (TypeError, ValueError):
                return None
        self.add('weather', {'date': iso(ts), 'air_temperature': num('AirTemp'), 'track_temperature': num('TrackTemp'),
                             'humidity': num('Humidity'), 'pressure': num('Pressure'),
                             'rainfall': int(num('Rainfall') or 0), 'wind_direction': num('WindDirection'),
                             'wind_speed': num('WindSpeed')})

    def snapshot(self):
        """Elke paar seconden gaten van alle rijders vastleggen, zoals OpenF1 dat doet.
        De feed stuurt alleen wijzigingen; zonder momentopnames lijkt een stabiel gat 'oud'."""
        with self.lock:
            if not self.connected or not self.session:
                return
            now = iso(datetime.now(timezone.utc))
            for num, L in ((self.state.get('TimingData') or {}).get('Lines') or {}).items():
                if isinstance(L, dict) and num.isdigit() and not L.get('Retired'):
                    self.add('intervals', {'date': now, 'driver_number': int(num),
                                           'gap_to_leader': gap_value(L.get('GapToLeader')),
                                           'interval': gap_value((L.get('IntervalToPositionAhead') or {}).get('Value'))})

    # --- opvragen
    def source(self, ep):
        if ep == 'sessions':
            return [dict(self.session)] if self.session else []
        if ep == 'drivers':
            return self.drivers()
        if ep == 'laps':
            return list(self.laps.values())
        if ep == 'stints':
            return [s for v in self.stints.values() for s in v]
        if ep == 'hud_status':
            return [{'connected': self.connected, 'session_key': (self.session or {}).get('session_key'),
                     'total_laps': self.total_laps, 'seconds_since_data': round(time.time() - self.last_msg, 1)
                     if self.last_msg else None}]
        return list(self.rows.get(ep, []))


LIVE = Live()


# ---------------------------------------------------------------- filters zoals OpenF1
FILTER = re.compile(r'^([a-z_0-9]+)(>=|<=|>|<|=)(.*)$')


def matches(rec, field, op, raw):
    if field in ('session_key', 'meeting_key') and raw == 'latest':
        return True
    v = rec.get(field)
    if v is None:
        return False
    if field.startswith('date'):
        a, b = utc(v), utc(raw)
    else:
        try:
            a, b = float(v), float(raw)
        except (TypeError, ValueError):
            a, b = str(v), raw
    if a is None or b is None:
        return False
    return {'=': a == b, '>=': a >= b, '<=': a <= b, '>': a > b, '<': a < b}[op]


def query(ep, qs):
    with LIVE.lock:
        rows = LIVE.source(ep)
    for part in filter(None, qs.split('&')):
        m = FILTER.match(unquote(part))
        if m:
            rows = [r for r in rows if matches(r, *m.groups())]
    return rows


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def send(self, code, body, ctype):
        self.send_response(code)
        self.send_header('Content-Type', ctype)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-store')
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        u = urlsplit(self.path)
        if u.path in ('/', '/index.html'):
            p = os.path.join(HERE, 'index.html')
            if not os.path.exists(p):
                return self.send(404, b'index.html niet gevonden naast f1hud_server.py', 'text/plain; charset=utf-8')
            with open(p, 'rb') as fh:
                return self.send(200, fh.read(), 'text/html; charset=utf-8')
        if u.path.startswith('/v1/'):
            ep = u.path[4:].strip('/')
            return self.send(200, json.dumps(query(ep, u.query)).encode(), 'application/json')
        self.send(404, b'[]', 'application/json')


# ---------------------------------------------------------------- verbinding met F1
def negotiate_cookie():
    req = urllib.request.Request(NEGOTIATE_URL, method='OPTIONS')
    with urllib.request.urlopen(req, timeout=15) as resp:
        for h in resp.headers.get_all('Set-Cookie') or []:
            if h.startswith('AWSALBCORS='):
                return h.split(';', 1)[0]
    return None


async def connect_ws(headers):
    try:
        return await websockets.connect(WS_URL, additional_headers=headers, ping_interval=None, max_size=2 ** 24)
    except TypeError:   # oudere versies van websockets
        return await websockets.connect(WS_URL, extra_headers=headers, ping_interval=None, max_size=2 ** 24)


async def snapshots():
    while True:
        await asyncio.sleep(4)
        LIVE.snapshot()


async def feed():
    asyncio.create_task(snapshots())
    delay = 2
    while True:
        try:
            cookie = await asyncio.to_thread(negotiate_cookie)
            ws = await connect_ws({'Cookie': cookie} if cookie else {})
            async with ws:
                await ws.send(json.dumps({'protocol': 'json', 'version': 1}) + RS)
                await ws.recv()
                await ws.send(json.dumps({'type': 1, 'invocationId': '1', 'target': 'Subscribe',
                                          'arguments': [TOPICS]}) + RS)
                LIVE.connected = True
                print('Verbonden met de F1 live-timingfeed.')
                delay = 2
                async for raw in ws:
                    for part in raw.split(RS):
                        if part.strip():
                            await handle_raw(ws, part)
        except Exception as e:      # verbinding weg of geweigerd: rustig opnieuw proberen
            LIVE.connected = False
            print(f'Geen verbinding met F1 ({type(e).__name__}: {e}). Nieuwe poging over {delay} s.')
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)


async def handle_raw(ws, part):
    try:
        msg = json.loads(part)
    except ValueError:
        return
    t = msg.get('type')
    now = datetime.now(timezone.utc)
    if t == 6:
        await ws.send(json.dumps({'type': 6}) + RS)
    elif t == 3 and isinstance(msg.get('result'), dict):
        result = msg['result']
        for topic in sorted(result, key=lambda x: x != 'SessionInfo'):   # SessionInfo eerst
            LIVE.handle(topic, result[topic], now, True)
        s = LIVE.session
        if s:
            print(f'Sessie: {s["location"]}, {s["session_name"]} (start {s["date_start"]} UTC).')
    elif t == 1 and msg.get('target') == 'feed':
        args = msg.get('arguments') or []
        if len(args) >= 2:
            LIVE.handle(args[0], args[1], (utc(args[2]) if len(args) > 2 else None) or now, False)
    elif t == 7:
        raise ConnectionError(msg.get('error') or 'server sloot de verbinding')


def lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(('8.8.8.8', 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except OSError:
        return '127.0.0.1'


def main():
    srv = ThreadingHTTPServer(('0.0.0.0', PORT), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print(f'F1 HUD draait. Open op je telefoon (zelfde wifi): http://{lan_ip()}:{PORT}')
    print('Stoppen met Ctrl+C.')
    try:
        asyncio.run(feed())
    except KeyboardInterrupt:
        print('Gestopt.')


if __name__ == '__main__':
    main()
