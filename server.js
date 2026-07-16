/**
 * FarmaControl — Servidor de produccion v4
 * - Persistencia en PostgreSQL (si existe DATABASE_URL) con fallback a JSON local
 * - Login validado en el servidor (credenciales por variables de entorno)
 * - WebSocket con eventos incrementales (scan, scan_venta, inv_*, venta)
 */
const http = require('http');
const fs   = require('fs');
const path = require('path');
const os   = require('os');
const { WebSocketServer } = require('ws');

const PORT    = process.env.PORT || 3000;
const PUBLIC  = path.join(__dirname, 'public');
const DB_FILE = path.join(__dirname, 'data', 'db.json');
const USE_PG  = !!process.env.DATABASE_URL;

// ── Configuración de negocio (por variables de entorno — una por cliente) ────
const NOMBRE_NEGOCIO = process.env.FC_NOMBRE || 'FarmaControl';
const USUARIOS = [
  { user: process.env.FC_USER1 || 'admin',    pass: process.env.FC_PASS1 || '12345',    rol: 'admin' },
  { user: process.env.FC_USER2 || 'operador', pass: process.env.FC_PASS2 || 'demo1234', rol: 'operador' }
];

function uid() {
  return Math.random().toString(36).slice(2, 10);
}

// ── Base de datos en memoria (siempre — para respuestas rápidas al cliente) ──
let db = { inventario: [], ventas: [], numeroFactura: 1 };

function dbDemo() {
  return {
    inventario: [
      { id: uid(), nombre: 'Paracetamol 500mg x10', cantidad: 200, precio: 40,  codigo: '', unidadesCaja: 100, precioCaja: 3500, unidadesSobre: 0, precioSobre: 0, cantUnidad: 0, cantSobre: 0, cantCaja: 2 },
      { id: uid(), nombre: 'Ibuprofeno 400mg x10',  cantidad: 120, precio: 45,  codigo: '', unidadesCaja: 100, precioCaja: 4000, unidadesSobre: 10, precioSobre: 420, cantUnidad: 0, cantSobre: 2, cantCaja: 1 },
      { id: uid(), nombre: 'Amoxicilina 500mg x7',  cantidad: 35,  precio: 320, codigo: '', unidadesCaja: 0, precioCaja: 0, unidadesSobre: 7, precioSobre: 2100, cantUnidad: 0, cantSobre: 5, cantCaja: 0 },
      { id: uid(), nombre: 'Jarabe Tos 120ml',       cantidad: 8,   precio: 1250, codigo: '', unidadesCaja: 0, precioCaja: 0, unidadesSobre: 0, precioSobre: 0, cantUnidad: 8, cantSobre: 0, cantCaja: 0 },
      { id: uid(), nombre: 'Loratadina 10mg x10',   cantidad: 150, precio: 58,  codigo: '', unidadesCaja: 0, precioCaja: 0, unidadesSobre: 10, precioSobre: 550, cantUnidad: 0, cantSobre: 15, cantCaja: 0 }
    ],
    ventas: [],
    numeroFactura: 1
  };
}

// ══════════════════════════════════════════════════════════════════════════
//  CAPA DE PERSISTENCIA — Postgres si hay DATABASE_URL, si no, JSON local
// ══════════════════════════════════════════════════════════════════════════
let pool = null;

async function initPersistencia() {
  if (USE_PG) {
    const { Pool } = require('pg');
    pool = new Pool({
      connectionString: process.env.DATABASE_URL,
      ssl: { rejectUnauthorized: false }
    });

    await pool.query(`
      CREATE TABLE IF NOT EXISTS inventario (
        id       TEXT PRIMARY KEY,
        nombre   TEXT NOT NULL,
        cantidad NUMERIC NOT NULL DEFAULT 0,
        precio   NUMERIC NOT NULL DEFAULT 0,
        codigo   TEXT DEFAULT '',
        unidad   TEXT DEFAULT 'Unidad',
        unidades_sobre NUMERIC DEFAULT 0,
        precio_sobre   NUMERIC DEFAULT 0,
        unidades_caja  NUMERIC DEFAULT 0,
        precio_caja    NUMERIC DEFAULT 0,
        cant_unidad NUMERIC DEFAULT 0,
        cant_sobre  NUMERIC DEFAULT 0,
        cant_caja   NUMERIC DEFAULT 0
      );
    `);
    // Migraciones seguras: agregan columnas nuevas sin borrar nada si la tabla ya existía.
    await pool.query(`ALTER TABLE inventario ADD COLUMN IF NOT EXISTS unidad TEXT DEFAULT 'Unidad';`);
    await pool.query(`ALTER TABLE inventario ADD COLUMN IF NOT EXISTS unidades_sobre NUMERIC DEFAULT 0;`);
    await pool.query(`ALTER TABLE inventario ADD COLUMN IF NOT EXISTS precio_sobre NUMERIC DEFAULT 0;`);
    await pool.query(`ALTER TABLE inventario ADD COLUMN IF NOT EXISTS unidades_caja NUMERIC DEFAULT 0;`);
    await pool.query(`ALTER TABLE inventario ADD COLUMN IF NOT EXISTS precio_caja NUMERIC DEFAULT 0;`);
    await pool.query(`ALTER TABLE inventario ADD COLUMN IF NOT EXISTS cant_unidad NUMERIC DEFAULT 0;`);
    await pool.query(`ALTER TABLE inventario ADD COLUMN IF NOT EXISTS cant_sobre NUMERIC DEFAULT 0;`);
    await pool.query(`ALTER TABLE inventario ADD COLUMN IF NOT EXISTS cant_caja NUMERIC DEFAULT 0;`);
    // Migración de datos: si un producto viejo tiene "cantidad" pero nunca se le asignó
    // desglose por presentación, se asume que todo lo que tenía era "unidades sueltas".
    await pool.query(`
      UPDATE inventario SET cant_unidad = cantidad
      WHERE cant_unidad = 0 AND cant_sobre = 0 AND cant_caja = 0 AND cantidad > 0;
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS ventas (
        id       TEXT PRIMARY KEY,
        fecha    TIMESTAMPTZ NOT NULL,
        factura  INTEGER NOT NULL,
        items    JSONB NOT NULL,
        total    NUMERIC NOT NULL
      );
    `);
    await pool.query(`
      CREATE TABLE IF NOT EXISTS meta (
        clave TEXT PRIMARY KEY,
        valor TEXT
      );
    `);

    const { rows: invRows } = await pool.query('SELECT * FROM inventario ORDER BY nombre ASC');
    const { rows: ventaRows } = await pool.query('SELECT * FROM ventas ORDER BY fecha ASC');
    const { rows: metaRows } = await pool.query(`SELECT valor FROM meta WHERE clave = 'numeroFactura'`);

    if (invRows.length === 0 && ventaRows.length === 0) {
      // Primera vez — sembrar datos demo
      const demo = dbDemo();
      for (const item of demo.inventario) {
        await pool.query(
          `INSERT INTO inventario (id, nombre, cantidad, precio, codigo, unidades_sobre, precio_sobre, unidades_caja, precio_caja, cant_unidad, cant_sobre, cant_caja)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
          [item.id, item.nombre, item.cantidad, item.precio, item.codigo,
           item.unidadesSobre || 0, item.precioSobre || 0, item.unidadesCaja || 0, item.precioCaja || 0,
           item.cantUnidad || 0, item.cantSobre || 0, item.cantCaja || 0]
        );
      }
      await pool.query(
        `INSERT INTO meta (clave, valor) VALUES ('numeroFactura','1')
         ON CONFLICT (clave) DO NOTHING`
      );
      db = demo;
      console.log('[DB-PG] Base de datos nueva — datos demo sembrados');
    } else {
      db.inventario = invRows.map(r => ({
        id: r.id, nombre: r.nombre, cantidad: Number(r.cantidad), precio: Number(r.precio), codigo: r.codigo || '',
        unidadesSobre: Number(r.unidades_sobre) || 0, precioSobre: Number(r.precio_sobre) || 0,
        unidadesCaja: Number(r.unidades_caja) || 0, precioCaja: Number(r.precio_caja) || 0,
        cantUnidad: Number(r.cant_unidad) || 0, cantSobre: Number(r.cant_sobre) || 0, cantCaja: Number(r.cant_caja) || 0
      }));
      db.ventas = ventaRows.map(r => ({
        id: r.id, fecha: r.fecha.toISOString(), factura: r.factura, items: r.items, total: Number(r.total)
      }));
      db.numeroFactura = metaRows.length ? (parseInt(metaRows[0].valor, 10) || 1) : 1;
      console.log(`[DB-PG] Cargado: ${db.inventario.length} productos, ${db.ventas.length} ventas`);
    }
  } else {
    // Fallback local — JSON en disco (uso en red local sin internet)
    try {
      if (fs.existsSync(DB_FILE)) {
        db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
        db.inventario = db.inventario.map(i => ({ id: i.id || uid(), ...i }));
        console.log(`[DB-JSON] Cargado: ${db.inventario.length} productos, ${db.ventas.length} ventas`);
      } else {
        db = dbDemo();
        guardarJSON();
        console.log('[DB-JSON] Datos demo creados (modo local, sin DATABASE_URL)');
      }
    } catch (e) {
      console.error('[DB-JSON] Error cargando, usando demo:', e.message);
      db = dbDemo();
    }
    console.warn('[AVISO] DATABASE_URL no configurada — usando JSON local. Esto NO es seguro en Railway/Render (se borra en cada redeploy). Configura una base Postgres antes de producción.');
  }
}

let _saveTimer = null;
function guardarJSON() {
  clearTimeout(_saveTimer);
  _saveTimer = setTimeout(() => {
    try {
      fs.mkdirSync(path.dirname(DB_FILE), { recursive: true });
      fs.writeFile(DB_FILE, JSON.stringify(db), err => {
        if (err) console.error('[DB-JSON] Error guardando:', err.message);
      });
    } catch (e) { console.error('[DB-JSON] Error:', e.message); }
  }, 300);
}

// Operaciones — siempre actualizan memoria YA (para que el broadcast sea instantáneo)
// y persisten en segundo plano (Postgres o JSON), sin bloquear el WebSocket.
async function persistirInvAdd(item) {
  if (USE_PG) {
    try {
      await pool.query(
        `INSERT INTO inventario (id, nombre, cantidad, precio, codigo, unidades_sobre, precio_sobre, unidades_caja, precio_caja, cant_unidad, cant_sobre, cant_caja)
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`,
        [item.id, item.nombre, item.cantidad, item.precio, item.codigo || '',
         item.unidadesSobre || 0, item.precioSobre || 0, item.unidadesCaja || 0, item.precioCaja || 0,
         item.cantUnidad || 0, item.cantSobre || 0, item.cantCaja || 0]
      );
    } catch (e) { console.error('[DB-PG] Error insertando producto:', e.message); }
  } else guardarJSON();
}

async function persistirInvUpdate(item) {
  if (USE_PG) {
    try {
      await pool.query(
        `UPDATE inventario SET nombre=$2, cantidad=$3, precio=$4, codigo=$5,
         unidades_sobre=$6, precio_sobre=$7, unidades_caja=$8, precio_caja=$9,
         cant_unidad=$10, cant_sobre=$11, cant_caja=$12 WHERE id=$1`,
        [item.id, item.nombre, item.cantidad, item.precio, item.codigo || '',
         item.unidadesSobre || 0, item.precioSobre || 0, item.unidadesCaja || 0, item.precioCaja || 0,
         item.cantUnidad || 0, item.cantSobre || 0, item.cantCaja || 0]
      );
    } catch (e) { console.error('[DB-PG] Error actualizando producto:', e.message); }
  } else guardarJSON();
}

async function persistirInvDelete(id) {
  if (USE_PG) {
    try { await pool.query('DELETE FROM inventario WHERE id=$1', [id]); }
    catch (e) { console.error('[DB-PG] Error borrando producto:', e.message); }
  } else guardarJSON();
}

async function persistirVenta(venta, numeroFactura, itemsActualizados) {
  if (USE_PG) {
    try {
      await pool.query(
        'INSERT INTO ventas (id, fecha, factura, items, total) VALUES ($1,$2,$3,$4,$5)',
        [venta.id, venta.fecha, venta.factura, JSON.stringify(venta.items), venta.total]
      );
      await pool.query(
        `INSERT INTO meta (clave, valor) VALUES ('numeroFactura', $1)
         ON CONFLICT (clave) DO UPDATE SET valor = $1`,
        [String(numeroFactura)]
      );
      for (const item of itemsActualizados) {
        await pool.query(
          'UPDATE inventario SET cantidad=$2, cant_unidad=$3, cant_sobre=$4, cant_caja=$5 WHERE id=$1',
          [item.id, item.cantidad, item.cantUnidad || 0, item.cantSobre || 0, item.cantCaja || 0]
        );
      }
    } catch (e) { console.error('[DB-PG] Error guardando venta:', e.message); }
  } else guardarJSON();
}

// ── HTTP ──────────────────────────────────────────────────────────────────────
const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js':   'application/javascript',
  '.css':  'text/css',
  '.png':  'image/png',
  '.jpg':  'image/jpeg',
  '.ico':  'image/x-icon',
  '.json': 'application/json',
  '.webmanifest': 'application/manifest+json'
};

function leerJSON(req, cb) {
  let body = '';
  req.on('data', chunk => { body += chunk; if (body.length > 1e6) req.destroy(); });
  req.on('end', () => {
    try { cb(null, JSON.parse(body || '{}')); }
    catch (e) { cb(e); }
  });
}

const server = http.createServer((req, res) => {
  const url = req.url.split('?')[0];

  // ── API: configuración pública del negocio (sin contraseñas) ──
  if (url === '/api/config' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
    return res.end(JSON.stringify({ nombre: NOMBRE_NEGOCIO }));
  }

  // ── API: login — valida en el servidor, nunca en el HTML ──
  if (url === '/api/login' && req.method === 'POST') {
    return leerJSON(req, (err, body) => {
      if (err) { res.writeHead(400); return res.end(JSON.stringify({ ok: false, error: 'JSON inválido' })); }
      const { usuario, clave } = body;
      const match = USUARIOS.find(u => u.user === usuario && u.pass === clave);
      res.writeHead(match ? 200 : 401, { 'Content-Type': 'application/json' });
      if (match) return res.end(JSON.stringify({ ok: true, usuario: match.user, rol: match.rol }));
      return res.end(JSON.stringify({ ok: false, error: 'Usuario o contraseña incorrectos' }));
    });
  }

  if (url === '/api/db' && req.method === 'GET') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-cache' });
    return res.end(JSON.stringify(db));
  }

  if (url === '/api/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, db: USE_PG ? 'postgres' : 'json-local', productos: db.inventario.length, ts: Date.now() }));
  }

  // Archivos estáticos
  const rel  = url === '/' ? '/index.html' : url;
  const file = path.join(PUBLIC, rel);
  if (!file.startsWith(PUBLIC)) { res.writeHead(403); return res.end('Forbidden'); }

  fs.readFile(file, (err, data) => {
    if (err) { res.writeHead(404); return res.end('Not found'); }
    const ext = path.extname(file).toLowerCase();
    res.writeHead(200, {
      'Content-Type': MIME[ext] || 'application/octet-stream',
      'Cache-Control': ext === '.html' ? 'no-cache' : 'max-age=3600'
    });
    res.end(data);
  });
});

// ── WebSocket ─────────────────────────────────────────────────────────────────
const wss = new WebSocketServer({ server, perMessageDeflate: false });
const clients = new Set();

function send(ws, obj) {
  if (ws.readyState === 1) ws.send(JSON.stringify(obj));
}

function broadcast(obj, skip) {
  const msg = JSON.stringify(obj);
  clients.forEach(c => { if (c !== skip && c.readyState === 1) c.send(msg); });
}

wss.on('connection', (ws, req) => {
  const ip = req.socket.remoteAddress;
  const params = new URLSearchParams((req.url || '').split('?')[1] || '');
  ws.rol = params.get('rol') === 'admin' ? 'admin' : 'operador';
  clients.add(ws);
  ws.isAlive = true;
  console.log(`[WS] + ${ip} (rol: ${ws.rol})  (${clients.size} conectados)`);

  send(ws, { t: 'db', db });

  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    let m;
    try { m = JSON.parse(raw); } catch { return; }

    switch (m.t) {

      // PRIORIDAD MAXIMA — código escaneado en módulo Escanear, reenvío instantáneo
      case 'scan':
        broadcast({ t: 'scan', codigo: m.codigo, ts: m.ts }, ws);
        break;

      // PRIORIDAD MAXIMA — código escaneado en módulo Ventas, reenvío instantáneo
      case 'scan_venta':
        broadcast({ t: 'scan_venta', codigo: m.codigo, ts: m.ts }, ws);
        break;

      case 'inv_add': {
        const item = { id: uid(), ...m.item };
        db.inventario.push(item);
        persistirInvAdd(item);
        broadcast({ t: 'inv_add', item }, ws);
        break;
      }

      case 'inv_update': {
        const idx = db.inventario.findIndex(i => i.id === m.item.id);
        if (idx >= 0) {
          db.inventario[idx] = { ...db.inventario[idx], ...m.item };
          persistirInvUpdate(db.inventario[idx]);
          broadcast({ t: 'inv_update', item: db.inventario[idx] }, ws);
        }
        break;
      }

      case 'inv_delete': {
        if (ws.rol !== 'admin') {
          send(ws, { t: 'permiso_denegado', accion: 'inv_delete' });
          console.log(`[SEGURIDAD] Intento de borrar producto rechazado (rol: ${ws.rol})`);
          break;
        }
        db.inventario = db.inventario.filter(i => i.id !== m.id);
        persistirInvDelete(m.id);
        broadcast({ t: 'inv_delete', id: m.id }, ws);
        break;
      }

      case 'venta': {
        db.ventas.push(m.venta);
        db.numeroFactura = m.numeroFactura;
        const itemsActualizados = [];
        m.venta.items.forEach(vi => {
          const prod = db.inventario.find(i => i.id === vi.id || i.nombre === vi.nombre);
          if (prod) {
            const cantVendida = Number(vi.cant) || 0;
            const pres = vi.presentacion || 'Unidad';
            if (pres === 'Caja') prod.cantCaja = Math.max(0, (prod.cantCaja || 0) - cantVendida);
            else if (pres === 'Sobre') prod.cantSobre = Math.max(0, (prod.cantSobre || 0) - cantVendida);
            else prod.cantUnidad = Math.max(0, (prod.cantUnidad || 0) - cantVendida);
            // El total en unidades base queda siempre sincronizado con los 3 buckets
            prod.cantidad = (prod.cantUnidad || 0)
              + (prod.cantSobre || 0) * (prod.unidadesSobre || 0)
              + (prod.cantCaja || 0) * (prod.unidadesCaja || 0);
            itemsActualizados.push(prod);
          }
        });
        persistirVenta(m.venta, m.numeroFactura, itemsActualizados);
        broadcast({ t: 'venta', venta: m.venta, numeroFactura: m.numeroFactura, inventario: db.inventario }, ws);
        send(ws, { t: 'inv_sync', inventario: db.inventario });
        break;
      }

      case 'get_db':
        send(ws, { t: 'db', db });
        break;

      case 'ping':
        send(ws, { t: 'pong', ts: Date.now() });
        break;
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[WS] - ${ip}  (${clients.size} conectados)`);
  });

  ws.on('error', e => console.error('[WS] Error:', e.message));
});

setInterval(() => {
  clients.forEach(ws => {
    if (!ws.isAlive) { ws.terminate(); clients.delete(ws); return; }
    ws.isAlive = false;
    ws.ping();
  });
}, 20000);

// ── Arranque ──────────────────────────────────────────────────────────────────
function getLocalIP() {
  for (const ifaces of Object.values(os.networkInterfaces())) {
    for (const i of ifaces) {
      if (i.family === 'IPv4' && !i.internal) return i.address;
    }
  }
  return 'localhost';
}

initPersistencia().then(() => {
  server.listen(PORT, '0.0.0.0', () => {
    const ip = getLocalIP();
    const pad = s => s.padEnd(38);
    console.log('\n╔══════════════════════════════════════════════════╗');
    console.log(`║   ${pad(NOMBRE_NEGOCIO + ' — Servidor v4')}║`);
    console.log('╠══════════════════════════════════════════════════╣');
    console.log(`║  Local  → ${pad('http://localhost:' + PORT)}║`);
    console.log(`║  Red    → ${pad('http://' + ip + ':' + PORT)}║`);
    console.log(`║  Datos  → ${pad(USE_PG ? 'PostgreSQL (persistente)' : 'JSON local (temporal)')}║`);
    console.log('╠══════════════════════════════════════════════════╣');
    console.log('║  Ctrl+C para detener                              ║');
    console.log('╚══════════════════════════════════════════════════╝\n');
  });
}).catch(e => {
  console.error('[FATAL] No se pudo iniciar la persistencia:', e.message);
  process.exit(1);
});
