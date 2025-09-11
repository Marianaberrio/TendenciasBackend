#!/usr/bin/env node
/**
 * CORE CLI (menu de consola) para consumir las APIs
 * - Sin dependencias externas (usa fetch nativo de Node 18+)
 * - Maneja sesión (access/refresh tokens) con auto-refresh
 * - Menús: Autenticación, Pacientes, Doctores, Recetas, Dispensación, Reportes
 * - Subcomandos: login | daemon | menu (por defecto: menu)
 *
 * Uso:
 *   node core.js               # abre menú interactivo
 *   node core.js menu          # idem
 *   node core.js login         # login interactivo (maneja MFA si aplica)
 *   node core.js daemon        # reemisión automática de QRs por expirar
 */

const fs = require('fs');
const path = require('path');
const rl = require('readline');
const os = require('os');
const cp = require('child_process');

const BASE = process.env.CORE_BASE || 'http://localhost:3000';
const SESSION_FILE = path.join(__dirname, '.core.session.json');

// ===================== util consola =====================
function prompt(question, { mask = false } = {}) {
  return new Promise((resolve) => {
    const r = rl.createInterface({ input: process.stdin, output: process.stdout, terminal: true });
    if (!mask) {
      r.question(question, (ans) => { r.close(); resolve(ans.trim()); });
    } else {
      // ocultar password
      const stdin = process.openStdin();
      process.stdout.write(question);
      let value = '';
      function onData(char) {
        char = char + '';
        switch (char) {
          case '\n':
          case '\r':
          case '\u0004':
            stdin.removeListener('data', onData);
            process.stdout.write(os.EOL);
            r.close();
            resolve(value.trim());
            break;
          case '\u0003': // Ctrl+C
            process.exit(1);
          default:
            process.stdout.write('*');
            value += char;
            break;
        }
      }
      stdin.on('data', onData);
    }
  });
}
async function pressEnter() { await prompt('\n(Enter para continuar) '); }

function printTable(rows) {
  if (!Array.isArray(rows) || rows.length === 0) {
    console.log('(sin resultados)');
    return;
  }
  const cols = Object.keys(rows[0]);
  const widths = cols.map(c => Math.max(c.length, ...rows.map(r => String(r[c] ?? '').length)));
  const sep = '+' + widths.map(w => '-'.repeat(w + 2)).join('+') + '+';
  console.log(sep);
  console.log('|' + cols.map((c,i)=>' '+c.padEnd(widths[i])+' ').join('|') + '|');
  console.log(sep);
  for (const row of rows) {
    console.log('|' + cols.map((c,i)=>' '+String(row[c] ?? '').padEnd(widths[i])+' ').join('|') + '|');
  }
  console.log(sep);
}
function toBool(v){ return v===true || v===1 || v==='1' || String(v).toLowerCase()==='true'; }

// ===================== helpers de entrada "bonitos" =====================
async function askMode(label = 'cuerpo') {
  console.log(`\n¿Cómo quieres ingresar el ${label}?`);
  console.log('1) Llenar por campos (form)');
  console.log('2) Pegar JSON');
  console.log('3) Cargar desde archivo (@ruta.json)');
  console.log('4) Abrir en editor con plantilla');
  let m;
  while (!['1','2','3','4'].includes(m = await prompt('Opción (1-4): '))) {}
  return m;
}
function pick(obj, allowed) {
  if (!Array.isArray(allowed) || allowed.length === 0) return obj;
  const out = {};
  for (const k of allowed) if (obj[k] !== undefined) out[k] = obj[k];
  return out;
}
function safeParseJSON(t) {
  try { return JSON.parse(t); } catch { return null; }
}
async function readJsonFromFileHint(hint) {
  if (!hint || !hint.startsWith('@')) return null;
  const p = hint.slice(1).trim();
  try {
    const txt = fs.readFileSync(path.resolve(p), 'utf8');
    const j = safeParseJSON(txt);
    if (!j) throw new Error('JSON inválido en archivo.');
    return j;
  } catch (e) {
    console.error('❌', e.message);
    return null;
  }
}
async function openInEditorWithTemplate(templateObj) {
  const tmp = path.join(os.tmpdir(), `corecli_${Date.now()}.json`);
  fs.writeFileSync(tmp, JSON.stringify(templateObj, null, 2), 'utf8');
  const editor = process.platform === 'win32' ? 'notepad' : (process.env.EDITOR || 'vi');
  console.log(`\nAbriendo ${tmp} en ${editor}... (guarda y cierra para continuar)`);
  try {
    cp.spawnSync(editor, [tmp], { stdio: 'inherit' });
  } catch {
    console.log(`No pude abrir el editor automáticamente. Ábrelo manualmente: ${tmp}`);
    await pressEnter();
  }
  const txt = fs.readFileSync(tmp, 'utf8');
  const j = safeParseJSON(txt);
  if (!j) throw new Error('El archivo guardado no es JSON válido.');
  return j;
}
/**
 * askBody: asistente genérico para construir cuerpos JSON
 * @param {Object} opts
 *  - label: texto para mostrar
 *  - fields: [{key, label, required, parse}]  // para modo formulario
 *  - template: objeto plantilla sugerida (se usa en editor)
 *  - allowedKeys: restringe las claves que se envían (sanitiza)
 */
async function askBody(opts = {}) {
  const { label = 'cuerpo', fields = [], template = {}, allowedKeys = [] } = opts;
  const mode = await askMode(label);

  if (mode === '1') {
    // formulario
    const out = {};
    for (const f of fields) {
      let v = await prompt(`${f.label || f.key}${f.required ? ' *' : ''}: `);
      if ((v === '' || v == null) && f.required) {
        console.log('Este campo es obligatorio.');
        v = await prompt(`${f.label || f.key} *: `);
      }
      if (f.parse) {
        try { v = f.parse(v); } catch { console.log('Valor inválido; dejo como texto.'); }
      }
      if (v !== '' && v != null) out[f.key] = v;
    }
    return pick(out, allowedKeys);
  }

  if (mode === '2') {
    const raw = await prompt(`Pega JSON (una línea): `);
    const j = safeParseJSON(raw);
    if (!j) { console.log('JSON inválido.'); return await askBody(opts); }
    return pick(j, allowedKeys);
  }

  if (mode === '3') {
    const hint = await prompt('Ruta del archivo (empieza con @, ej: @C:\\data\\obj.json): ');
    const j = await readJsonFromFileHint(hint);
    if (!j) { console.log('No pude leer JSON del archivo.'); return await askBody(opts); }
    return pick(j, allowedKeys);
  }

  // 4) Editor
  try {
    const j = await openInEditorWithTemplate(template);
    return pick(j, allowedKeys);
  } catch (e) {
    console.error('❌', e.message);
    return await askBody(opts);
  }
}

// ===================== sesión =====================
function loadSession() {
  try { return JSON.parse(fs.readFileSync(SESSION_FILE, 'utf8')); }
  catch { return { access_token:null, refresh_token:null, me:null }; }
}
function saveSession(s) {
  fs.writeFileSync(SESSION_FILE, JSON.stringify(s, null, 2), 'utf8');
}
function clearSession() {
  saveSession({ access_token:null, refresh_token:null, me:null });
}

// ===================== HTTP (con auto-refresh) =====================
async function api(method, url, body, { auth = true } = {}) {
  const s = loadSession();
  const headers = { 'Content-Type': 'application/json' };
  if (auth && s.access_token) headers['Authorization'] = `Bearer ${s.access_token}`;
  const res = await fetch(BASE + url, {
    method,
    headers,
    body: body ? JSON.stringify(body) : undefined,
  });
  if (res.status === 401 && auth && s.refresh_token) {
    // intentar refresh una vez
    const ok = await tryRefresh();
    if (ok) {
      const s2 = loadSession();
      const headers2 = { 'Content-Type': 'application/json', 'Authorization': `Bearer ${s2.access_token}` };
      const res2 = await fetch(BASE + url, { method, headers: headers2, body: body ? JSON.stringify(body) : undefined });
      if (!res2.ok) throw await errorFrom(res2);
      return res2.json();
    }
  }
  if (!res.ok) throw await errorFrom(res);
  if (res.status === 204) return null;
  return res.json();
}
async function errorFrom(res) {
  let t;
  try { t = await res.json(); } catch { t = { error: res.statusText }; }
  return new Error(`${res.status} ${res.statusText} :: ${JSON.stringify(t)}`);
}
async function tryRefresh() {
  const s = loadSession();
  if (!s.refresh_token) return false;
  try {
    const res = await fetch(BASE + '/auth/refresh', {
      method: 'POST',
      headers: {'Content-Type':'application/json'},
      body: JSON.stringify({ refresh_token: s.refresh_token })
    });
    if (!res.ok) { clearSession(); return false; }
    const j = await res.json();
    saveSession({ ...s, access_token: j.access_token, refresh_token: j.refresh_token ?? s.refresh_token });
    return true;
  } catch { return false; }
}

// ===================== LOGIN / AUTENTICACIÓN =====================
async function doLoginInteractive() {
  const user = await prompt('Usuario: ');
  const pass = await prompt('Contraseña: ', { mask: true });

  // fase 1
  const r1 = await api('POST', '/auth/login', { nombre_usuario: user, contrasena: pass }, { auth:false });
  if (r1.need_mfa) {
    console.log('MFA requerido. Abre tu app TOTP.');
    const code = await prompt('Código TOTP: ');
    const r2 = await api('POST', '/auth/mfa/verify', { login_challenge: r1.login_challenge, code }, { auth:false });
    saveSession({ access_token: r2.access_token, refresh_token: r2.refresh_token, me: { nombre_usuario: user } });
    console.log('✅ Login OK (con MFA)');
  } else {
    saveSession({ access_token: r1.access_token, refresh_token: r1.refresh_token, me: { nombre_usuario: user } });
    console.log('✅ Login OK');
  }
}

async function authMenu() {
  while (true) {
    console.clear();
    console.log('=== AUTENTICACIÓN ===');
    console.log('1) Login');
    console.log('2) Ver /auth/me');
    console.log('3) Logout (revocar refresh actual si lo pasas)');
    console.log('4) Listar sesiones (/auth/sessions)');
    console.log('5) Revocar una sesión por id (/auth/sessions/:id)');
    console.log('6) Logout en todos los dispositivos (/auth/logout-all)');
    console.log('7) MFA setup (/auth/mfa/setup)');
    console.log('8) MFA enable (/auth/mfa/enable)');
    console.log('0) Volver');
    const op = await prompt('\nOpción: ');
    try {
      if (op === '1') { await doLoginInteractive(); await pressEnter(); }
      else if (op === '2') { const me = await api('GET','/auth/me'); console.log(me); await pressEnter(); }
      else if (op === '3') {
        const s = loadSession();
        if (!s.refresh_token) { console.log('No hay refresh_token guardado.'); }
        else {
          await api('POST','/auth/logout',{ refresh_token: s.refresh_token });
          clearSession();
          console.log('Sesión cerrada.');
        }
        await pressEnter();
      }
      else if (op === '4') {
        const rows = await api('GET','/auth/sessions');
        printTable(rows);
        await pressEnter();
      }
      else if (op === '5') {
        const id = await prompt('ID de sesión a revocar: ');
        const res = await api('DELETE', `/auth/sessions/${encodeURIComponent(id)}`);
        console.log(res);
        await pressEnter();
      }
      else if (op === '6') {
        const res = await api('POST','/auth/logout-all',{});
        console.log(res);
        await pressEnter();
      }
      else if (op === '7') {
        const j = await api('POST','/auth/mfa/setup',{});
        console.log('\nEscanea este otpauth_url en Google Authenticator/1Password:\n', j.otpauth_url);
        console.log('\nSecreto base32:', j.base32);
        await pressEnter();
      }
      else if (op === '8') {
        const code = await prompt('Código TOTP: ');
        const j = await api('POST','/auth/mfa/enable',{ code });
        console.log(j);
        await pressEnter();
      }
      else if (op === '0') return;
    } catch (e) {
      console.error('❌', e.message);
      await pressEnter();
    }
  }
}

// ===================== PACIENTES =====================
async function pacientesMenu() {
  while (true) {
    console.clear();
    console.log('=== PACIENTES ===');
    console.log('1) Crear');
    console.log('2) Listar');
    console.log('3) Ver por ID');
    console.log('4) Actualizar (PATCH) por ID');
    console.log('5) Eliminar por ID');
    console.log('6) Ver por cédula');
    console.log('7) Actualizar por cédula');
    console.log('8) Eliminar por cédula');
    console.log('0) Volver');
    const op = await prompt('\nOpción: ');
    try {
      if (op === '1') {
        // DB: identificacion_pac (req), nombre_pac (req), apellido_pac (req),
        // fecha_nac_pac (req), num_seguro_pac (opt), direccion_pac (opt),
        // telefono_pac (opt), correo_pac (opt), sexo_pac (opt: O|F|M)
        const body = await askBody({
          label: 'paciente',
          fields: [
            { key: 'identificacion_pac', label: 'Cédula/identificación', required: true },
            { key: 'nombre_pac', label: 'Nombre', required: true },
            { key: 'apellido_pac', label: 'Apellido', required: true },
            { key: 'fecha_nac_pac', label: 'Fecha de nacimiento (YYYY-MM-DD)', required: true },
            { key: 'num_seguro_pac', label: 'Número de seguro' },
            { key: 'direccion_pac', label: 'Dirección' },
            { key: 'telefono_pac', label: 'Teléfono' },
            { key: 'correo_pac', label: 'Correo' },
            { key: 'sexo_pac', label: 'Sexo (M/F/O)' }
          ],
          template: {
            identificacion_pac: "V-123",
            nombre_pac: "Ana",
            apellido_pac: "Pérez",
            fecha_nac_pac: "1990-05-10",
            num_seguro_pac: "S-0001",
            direccion_pac: "Av. Principal #123",
            telefono_pac: "809-000-0000",
            correo_pac: "ana@example.com",
            sexo_pac: "F"
          },
          allowedKeys: [
            'identificacion_pac','nombre_pac','apellido_pac','fecha_nac_pac',
            'num_seguro_pac','direccion_pac','telefono_pac','correo_pac','sexo_pac'
          ]
        });
        const r = await api('POST','/api/pacientes',body);
        console.log('Creado:', r);
        await pressEnter();
      } else if (op === '2') {
        const limit = await prompt('limit (def 50): ');
        const offset = await prompt('offset (def 0): ');
        const r = await api('GET', `/api/pacientes?limit=${encodeURIComponent(limit||'50')}&offset=${encodeURIComponent(offset||'0')}`);
        printTable(r);
        await pressEnter();
      } else if (op === '3') {
        const id = await prompt('ID: ');
        const r = await api('GET', `/api/pacientes/${encodeURIComponent(id)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '4') {
        const id = await prompt('ID: ');
        const body = await askBody({
          label: 'actualización de paciente',
          fields: [
            { key: 'identificacion_pac', label: 'Cédula/identificación' },
            { key: 'nombre_pac', label: 'Nombre' },
            { key: 'apellido_pac', label: 'Apellido' },
            { key: 'fecha_nac_pac', label: 'Fecha de nacimiento (YYYY-MM-DD)' },
            { key: 'num_seguro_pac', label: 'Número de seguro' },
            { key: 'direccion_pac', label: 'Dirección' },
            { key: 'telefono_pac', label: 'Teléfono' },
            { key: 'correo_pac', label: 'Correo' },
            { key: 'sexo_pac', label: 'Sexo (M/F/O)' }
          ],
          template: {
            identificacion_pac: "", nombre_pac: "", apellido_pac: "",
            fecha_nac_pac: "", num_seguro_pac: "", direccion_pac: "",
            telefono_pac: "", correo_pac: "", sexo_pac: ""
          },
          allowedKeys: [
            'identificacion_pac','nombre_pac','apellido_pac','fecha_nac_pac',
            'num_seguro_pac','direccion_pac','telefono_pac','correo_pac','sexo_pac'
          ]
        });
        const r = await api('PATCH', `/api/pacientes/${encodeURIComponent(id)}`, body);
        console.log(r);
        await pressEnter();
      } else if (op === '5') {
        const id = await prompt('ID: ');
        const r = await api('DELETE', `/api/pacientes/${encodeURIComponent(id)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '6') {
        const ced = await prompt('Cédula: ');
        const r = await api('GET', `/api/pacientes/cedula/${encodeURIComponent(ced)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '7') {
        const ced = await prompt('Cédula: ');
        const body = await askBody({
          label: 'actualización de paciente por cédula',
          fields: [
            { key: 'nombre_pac', label: 'Nombre' },
            { key: 'apellido_pac', label: 'Apellido' },
            { key: 'fecha_nac_pac', label: 'Fecha de nacimiento (YYYY-MM-DD)' },
            { key: 'num_seguro_pac', label: 'Número de seguro' },
            { key: 'direccion_pac', label: 'Dirección' },
            { key: 'telefono_pac', label: 'Teléfono' },
            { key: 'correo_pac', label: 'Correo' },
            { key: 'sexo_pac', label: 'Sexo (M/F/O)' }
          ],
          template: {
            nombre_pac:"", apellido_pac:"", fecha_nac_pac:"",
            num_seguro_pac:"", direccion_pac:"", telefono_pac:"",
            correo_pac:"", sexo_pac:""
          },
          allowedKeys: [
            'nombre_pac','apellido_pac','fecha_nac_pac',
            'num_seguro_pac','direccion_pac','telefono_pac','correo_pac','sexo_pac'
          ]
        });
        const r = await api('PATCH', `/api/pacientes/cedula/${encodeURIComponent(ced)}`, body);
        console.log(r);
        await pressEnter();
      } else if (op === '8') {
        const ced = await prompt('Cédula: ');
        const r = await api('DELETE', `/api/pacientes/cedula/${encodeURIComponent(ced)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '0') return;
    } catch (e) {
      console.error('❌', e.message);
      await pressEnter();
    }
  }
}

// ===================== DOCTORES =====================
async function doctoresMenu() {
  while (true) {
    console.clear();
    console.log('=== DOCTORES ===');
    console.log('1) Crear');
    console.log('2) Listar');
    console.log('3) Ver por ID');
    console.log('4) Actualizar (PATCH) por ID');
    console.log('5) Eliminar por ID');
    console.log('6) Ver por cédula');
    console.log('7) Actualizar por cédula');
    console.log('8) Eliminar por cédula');
    console.log('0) Volver');
    const op = await prompt('\nOpción: ');
    try {
      if (op === '1') {
        // DB: num_licencia_med (req), cedula_doc (req), nombre_doc (req),
        // apellido_doc (req), fecha_nac_doc (req), sexo_doc (opt O|F|M),
        // especialidad_doc (opt), telefono_doc (opt), correo_doc (opt)
        const body = await askBody({
          label: 'doctor',
          fields: [
            { key: 'num_licencia_med', label: 'N° Licencia', required: true },
            { key: 'cedula_doc', label: 'Cédula', required: true },
            { key: 'nombre_doc', label: 'Nombre', required: true },
            { key: 'apellido_doc', label: 'Apellido', required: true },
            { key: 'fecha_nac_doc', label: 'Fecha de nacimiento (YYYY-MM-DD)', required: true },
            { key: 'sexo_doc', label: 'Sexo (M/F/O)' },
            { key: 'especialidad_doc', label: 'Especialidad' },
            { key: 'telefono_doc', label: 'Teléfono' },
            { key: 'correo_doc', label: 'Correo' }
          ],
          template: {
            num_licencia_med: "X1",
            cedula_doc: "V-999",
            nombre_doc: "Juan",
            apellido_doc: "Pérez",
            fecha_nac_doc: "1985-03-20",
            sexo_doc: "M",
            especialidad_doc: "Medicina General",
            telefono_doc: "809-111-1111",
            correo_doc: "juan@example.com"
          },
          allowedKeys: [
            'num_licencia_med','cedula_doc','nombre_doc','apellido_doc',
            'fecha_nac_doc','sexo_doc','especialidad_doc','telefono_doc','correo_doc'
          ]
        });
        const r = await api('POST','/api/doctores',body);
        console.log('Creado:', r);
        await pressEnter();
      } else if (op === '2') {
        const limit = await prompt('limit (def 50): ');
        const offset = await prompt('offset (def 0): ');
        const r = await api('GET', `/api/doctores?limit=${encodeURIComponent(limit||'50')}&offset=${encodeURIComponent(offset||'0')}`);
        printTable(r);
        await pressEnter();
      } else if (op === '3') {
        const id = await prompt('ID (codigo_doctor): ');
        const r = await api('GET', `/api/doctores/${encodeURIComponent(id)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '4') {
        const id = await prompt('ID (codigo_doctor): ');
        const body = await askBody({
          label: 'actualización de doctor',
          fields: [
            { key: 'num_licencia_med', label: 'N° Licencia' },
            { key: 'cedula_doc', label: 'Cédula' },
            { key: 'nombre_doc', label: 'Nombre' },
            { key: 'apellido_doc', label: 'Apellido' },
            { key: 'fecha_nac_doc', label: 'Fecha de nacimiento (YYYY-MM-DD)' },
            { key: 'sexo_doc', label: 'Sexo (M/F/O)' },
            { key: 'especialidad_doc', label: 'Especialidad' },
            { key: 'telefono_doc', label: 'Teléfono' },
            { key: 'correo_doc', label: 'Correo' }
          ],
          template: {
            num_licencia_med:"", cedula_doc:"", nombre_doc:"", apellido_doc:"",
            fecha_nac_doc:"", sexo_doc:"", especialidad_doc:"", telefono_doc:"", correo_doc:""
          },
          allowedKeys: [
            'num_licencia_med','cedula_doc','nombre_doc','apellido_doc',
            'fecha_nac_doc','sexo_doc','especialidad_doc','telefono_doc','correo_doc'
          ]
        });
        const r = await api('PATCH', `/api/doctores/${encodeURIComponent(id)}`, body);
        console.log(r);
        await pressEnter();
      } else if (op === '5') {
        const id = await prompt('ID (codigo_doctor): ');
        const r = await api('DELETE', `/api/doctores/${encodeURIComponent(id)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '6') {
        const ced = await prompt('Cédula: ');
        const r = await api('GET', `/api/doctores/cedula/${encodeURIComponent(ced)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '7') {
        const ced = await prompt('Cédula: ');
        const body = await askBody({
          label: 'actualización de doctor por cédula',
          fields: [
            { key: 'num_licencia_med', label: 'N° Licencia' },
            { key: 'nombre_doc', label: 'Nombre' },
            { key: 'apellido_doc', label: 'Apellido' },
            { key: 'fecha_nac_doc', label: 'Fecha de nacimiento (YYYY-MM-DD)' },
            { key: 'sexo_doc', label: 'Sexo (M/F/O)' },
            { key: 'especialidad_doc', label: 'Especialidad' },
            { key: 'telefono_doc', label: 'Teléfono' },
            { key: 'correo_doc', label: 'Correo' }
          ],
          template: {
            num_licencia_med:"", nombre_doc:"", apellido_doc:"",
            fecha_nac_doc:"", sexo_doc:"", especialidad_doc:"", telefono_doc:"", correo_doc:""
          },
          allowedKeys: [
            'num_licencia_med','nombre_doc','apellido_doc','fecha_nac_doc',
            'sexo_doc','especialidad_doc','telefono_doc','correo_doc'
          ]
        });
        const r = await api('PATCH', `/api/doctores/cedula/${encodeURIComponent(ced)}`, body);
        console.log(r);
        await pressEnter();
      } else if (op === '8') {
        const ced = await prompt('Cédula: ');
        const r = await api('DELETE', `/api/doctores/cedula/${encodeURIComponent(ced)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '0') return;
    } catch (e) {
      console.error('❌', e.message);
      await pressEnter();
    }
  }
}

// ===================== RECETAS =====================
async function recetasMenu() {
  while (true) {
    console.clear();
    console.log('=== RECETAS ===');
    console.log('1) Crear receta (+items opcional, genera QR opcional)');
    console.log('2) Ver por código (detalle + joins)');
    console.log('3) Validar QR por jti (/api/recetas/qr/:jti)');
    console.log('4) Dispensar por código (PUT /recetas/:id/dispensar)');
    console.log('5) Cambiar estado (ISSUED|DISPENSED|REVOKED)');
    console.log('6) Emitir QR (si no tiene)  /reemitir (siempre nuevo)');
    console.log('7) Listar por paciente');
    console.log('8) Listar por doctor');
    console.log('9) Listado general (filtros)');
    console.log('10) Próximas a vencer (por QR)');
    console.log('11) Existe? (codigo_receta o rx_hash)');
    console.log('12) Auditoría');
    console.log('0) Volver');
    const op = await prompt('\nOpción: ');
    try {
      if (op === '1') {
        // DB requiere: id_paciente, codigo_doctor, created_by
        // También existen: estado_receta (CHECK), rx_hash (NOT NULL) pero puede generarlo el backend.
        const body = await askBody({
          label: 'receta',
          fields: [
            { key: 'id_paciente', label: 'ID Paciente', required: true, parse: v => Number(v) },
            { key: 'codigo_doctor', label: 'Código doctor', required: true, parse: v => Number(v) },
            { key: 'created_by', label: 'User ID creador', required: true, parse: v => Number(v) },
            { key: 'diagnostico', label: 'Diagnóstico' },
            { key: 'estado_receta', label: 'Estado (ISSUED|DISPENSED|REVOKED) (def: ISSUED)' },
            { key: 'rx_hash', label: 'rx_hash (64 hex) (opcional si lo genera el backend)' },
            { key: 'qr_emitir', label: '¿Emitir QR? (true/false)', parse: v => toBool(v) },
            { key: 'qr_ttl_hours', label: 'TTL QR (horas)', parse: v => v?Number(v):undefined }
          ],
          template: {
            id_paciente: 1,
            codigo_doctor: 1,
            created_by: 1,
            diagnostico: "Dx",
            estado_receta: "ISSUED",
            rx_hash: "",
            qr_emitir: true,
            qr_ttl_hours: 24,
            items: [
              { codigo_medicamento: 101, nombre_medicamento: "Amoxi", dosis_medicamento: "1 c/8h" }
            ]
          },
          allowedKeys: [
            'id_paciente','codigo_doctor','created_by','diagnostico',
            'estado_receta','rx_hash','qr_emitir','qr_ttl_hours','items'
          ]
        });
        // si no se proveyó estado, forzamos ISSUED por defecto
        if (!body.estado_receta) body.estado_receta = 'ISSUED';
        const r = await api('POST','/api/recetas', body);
        console.log('Creada:', r);
        await pressEnter();
      } else if (op === '2') {
        const id = await prompt('Código receta (RX-...): ');
        const r = await api('GET', `/api/recetas/${encodeURIComponent(id)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '3') {
        const jti = await prompt('QR jti: ');
        const r = await api('GET', `/api/recetas/qr/${encodeURIComponent(jti)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '4') {
        const id = await prompt('Código receta (RX-...): ');
        const tx = await prompt('onchain_tx_dispense (opcional): ');
        const ph = await prompt('codigo_farmacia (opcional): ');
        const r = await api('PUT', `/api/recetas/${encodeURIComponent(id)}/dispensar`, {
          onchain_tx_dispense: tx || null,
          qr_scanned_by_pharmacy_id: ph ? parseInt(ph,10) : null
        });
        console.log(r);
        await pressEnter();
      } else if (op === '5') {
        const id = await prompt('Código receta (RX-...): ');
        const e = await prompt('Nuevo estado (ISSUED|DISPENSED|REVOKED): ');
        const r = await api('PUT', `/api/recetas/${encodeURIComponent(id)}/estado`, { estado: e });
        console.log(r);
        await pressEnter();
      } else if (op === '6') {
        const id = await prompt('Código receta (RX-...): ');
        const tipo = await prompt('emitir (1) o reemitir (2)? ');
        const ttl = await prompt('ttl_hours (def 168): ');
        if (tipo === '1') {
          const ovr = await prompt('override_if_exists? true/false (def false): ');
          const r = await api('POST', `/api/recetas/${encodeURIComponent(id)}/qr/emitir`, {
            ttl_hours: ttl ? Number(ttl) : 168,
            override_if_exists: toBool(ovr)
          });
          console.log(r);
        } else {
          const r = await api('POST', `/api/recetas/${encodeURIComponent(id)}/qr/reemitir`, {
            ttl_hours: ttl ? Number(ttl) : 168
          });
          console.log(r);
        }
        await pressEnter();
      } else if (op === '7') {
        const pid = await prompt('id_paciente: ');
        const estado = await prompt('estado (opcional): ');
        const desde = await prompt('desde YYYY-MM-DD (opcional): ');
        const hasta = await prompt('hasta YYYY-MM-DD (opcional): ');
        const limit = await prompt('limit (def 50): ');
        const offset = await prompt('offset (def 0): ');
        const q = `/api/pacientes/${encodeURIComponent(pid)}/recetas?estado=${encodeURIComponent(estado||'')}&desde=${encodeURIComponent(desde||'')}&hasta=${encodeURIComponent(hasta||'')}&limit=${encodeURIComponent(limit||'50')}&offset=${encodeURIComponent(offset||'0')}`;
        const r = await api('GET', q);
        printTable(r);
        await pressEnter();
      } else if (op === '8') {
        const did = await prompt('codigo_doctor: ');
        const estado = await prompt('estado (opcional): ');
        const desde = await prompt('desde YYYY-MM-DD (opcional): ');
        const hasta = await prompt('hasta YYYY-MM-DD (opcional): ');
        const limit = await prompt('limit (def 50): ');
        const offset = await prompt('offset (def 0): ');
        const q = `/api/doctores/${encodeURIComponent(did)}/recetas?estado=${encodeURIComponent(estado||'')}&desde=${encodeURIComponent(desde||'')}&hasta=${encodeURIComponent(hasta||'')}&limit=${encodeURIComponent(limit||'50')}&offset=${encodeURIComponent(offset||'0')}`;
        const r = await api('GET', q);
        printTable(r);
        await pressEnter();
      } else if (op === '9') {
        const estado = await prompt('estado (opcional): ');
        const paciente = await prompt('id_paciente (opcional): ');
        const doctor = await prompt('codigo_doctor (opcional): ');
        const desde = await prompt('desde YYYY-MM-DD (opcional): ');
        const hasta = await prompt('hasta YYYY-MM-DD (opcional): ');
        const campo = await prompt('campo fecha_receta|created_at (def fecha_receta): ');
        const order = await prompt('order asc|desc (def desc): ');
        const limit = await prompt('limit (def 50): ');
        const offset = await prompt('offset (def 0): ');
        const q = `/api/recetas?estado=${encodeURIComponent(estado||'')}&paciente=${encodeURIComponent(paciente||'')}&doctor=${encodeURIComponent(doctor||'')}&desde=${encodeURIComponent(desde||'')}&hasta=${encodeURIComponent(hasta||'')}&campo=${encodeURIComponent(campo||'')}&order=${encodeURIComponent(order||'')}&limit=${encodeURIComponent(limit||'50')}&offset=${encodeURIComponent(offset||'0')}`;
        const r = await api('GET', q);
        printTable(r);
        await pressEnter();
      } else if (op === '10') {
        const dias = await prompt('días (def 7): ');
        const limit = await prompt('limit (def 50): ');
        const offset = await prompt('offset (def 0): ');
        const q = `/api/recetas/vencen?dias=${encodeURIComponent(dias||'7')}&limit=${encodeURIComponent(limit||'50')}&offset=${encodeURIComponent(offset||'0')}`;
        const r = await api('GET', q);
        printTable(r);
        await pressEnter();
      } else if (op === '11') {
        const codigo = await prompt('codigo_receta (vacío si usarás rx_hash): ');
        const hash = await prompt('rx_hash (vacío si usas codigo): ');
        const q = `/api/recetas/exists?codigo_receta=${encodeURIComponent(codigo||'')}&rx_hash=${encodeURIComponent(hash||'')}`;
        const r = await api('GET', q);
        console.log(r);
        await pressEnter();
      } else if (op === '12') {
        const id = await prompt('Código receta (RX-...): ');
        const r = await api('GET', `/api/recetas/${encodeURIComponent(id)}/auditoria`);
        console.log(JSON.stringify(r,null,2));
        await pressEnter();
      } else if (op === '0') return;
    } catch (e) {
      console.error('❌', e.message);
      await pressEnter();
    }
  }
}

// ===================== DISPENSACIÓN =====================
async function dispMenu() {
  while (true) {
    console.clear();
    console.log('=== DISPENSACIÓN ===');
    console.log('1) Registrar dispensación (POST /api/dispensacion)');
    console.log('2) Registrar por QR jti (POST /api/dispensacion/qr/:jti)');
    console.log('3) Listar (filtros)');
    console.log('4) Contar (mismos filtros)');
    console.log('5) Detalle por num');
    console.log('6) Actualizar (sospecha/comentario)');
    console.log('7) Eliminar por num');
    console.log('8) Historial por receta (/api/recetas/:id/dispensacion)');
    console.log('0) Volver');
    const op = await prompt('\nOpción: ');
    try {
      if (op === '1') {
        // Tabla Dispensacion: codigo_receta (req), fecha_escaneado (req, tiene default), actividad_sospechosa (req, default 0), comentario (opt)
        const b = await askBody({
          label: 'dispensación',
          fields: [
            { key: 'codigo_receta', label: 'Código receta (RX-...)', required: true },
            { key: 'fecha_escaneado', label: 'Fecha ISO (YYYY-MM-DDTHH:mm:ssZ) (opcional si DB default)' },
            { key: 'actividad_sospechosa', label: '¿Sospechosa? (true/false)', parse: v => v===''?undefined:toBool(v) },
            { key: 'comentario', label: 'Comentario' }
          ],
          template: {
            codigo_receta: "RX-00001",
            fecha_escaneado: "",
            actividad_sospechosa: false,
            comentario: "OK"
          },
          allowedKeys: ['codigo_receta','fecha_escaneado','actividad_sospechosa','comentario']
        });
        const r = await api('POST','/api/dispensacion', b);
        console.log(r);
        await pressEnter();
      } else if (op === '2') {
        const jti = await prompt('QR jti: ');
        const b = await askBody({
          label: 'dispensación por QR',
          fields: [
            { key: 'fecha_escaneado', label: 'Fecha ISO (YYYY-MM-DDTHH:mm:ssZ) (opcional si DB default)' },
            { key: 'actividad_sospechosa', label: '¿Sospechosa? (true/false)', parse: v => v===''?undefined:toBool(v) },
            { key: 'comentario', label: 'Comentario' }
          ],
          template: {
            fecha_escaneado: "",
            actividad_sospechosa: false,
            comentario: "OK"
          },
          allowedKeys: ['fecha_escaneado','actividad_sospechosa','comentario']
        });
        const r = await api('POST', `/api/dispensacion/qr/${encodeURIComponent(jti)}`, b);
        console.log(r);
        await pressEnter();
      } else if (op === '3') {
        // filtros soportados por columnas de Dispensacion
        const receta = await prompt('codigo_receta (opcional): ');
        const sospe = await prompt('sospechosa true|false (opcional): ');
        const desde = await prompt('desde YYYY-MM-DD (opcional): ');
        const hasta = await prompt('hasta YYYY-MM-DD (opcional): ');
        const limit = await prompt('limit (def 50): ');
        const offset = await prompt('offset (def 0): ');
        const order = await prompt('order asc|desc (def desc): ');
        const q = `/api/dispensacion?receta=${encodeURIComponent(receta||'')}`
                + `&sospechosa=${encodeURIComponent(sospe||'')}`
                + `&desde=${encodeURIComponent(desde||'')}&hasta=${encodeURIComponent(hasta||'')}`
                + `&limit=${encodeURIComponent(limit||'50')}&offset=${encodeURIComponent(offset||'0')}`
                + `&order=${encodeURIComponent(order||'desc')}`;
        const r = await api('GET', q);
        printTable(r);
        await pressEnter();
      } else if (op === '4') {
        const receta = await prompt('codigo_receta (opcional): ');
        const sospe = await prompt('sospechosa true|false (opcional): ');
        const desde = await prompt('desde YYYY-MM-DD (opcional): ');
        const hasta = await prompt('hasta YYYY-MM-DD (opcional): ');
        const q = `/api/dispensacion/count?receta=${encodeURIComponent(receta||'')}`
                + `&sospechosa=${encodeURIComponent(sospe||'')}`
                + `&desde=${encodeURIComponent(desde||'')}&hasta=${encodeURIComponent(hasta||'')}`;
        const r = await api('GET', q);
        console.log(r);
        await pressEnter();
      } else if (op === '5') {
        const num = await prompt('num_dispensacion: ');
        const r = await api('GET', `/api/dispensacion/${encodeURIComponent(num)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '6') {
        const num = await prompt('num_dispensacion: ');
        const b = await askBody({
          label: 'patch de dispensación',
          fields: [
            { key: 'actividad_sospechosa', label: '¿Sospechosa? (true/false)', parse: v => v===''?undefined:toBool(v) },
            { key: 'comentario', label: 'Comentario' }
          ],
          template: { actividad_sospechosa: false, comentario: "" },
          allowedKeys: ['actividad_sospechosa','comentario']
        });
        const r = await api('PATCH', `/api/dispensacion/${encodeURIComponent(num)}`, b);
        console.log(r);
        await pressEnter();
      } else if (op === '7') {
        const num = await prompt('num_dispensacion: ');
        const r = await api('DELETE', `/api/dispensacion/${encodeURIComponent(num)}`);
        console.log(r);
        await pressEnter();
      } else if (op === '8') {
        const id = await prompt('codigo_receta (RX-...): ');
        const r = await api('GET', `/api/recetas/${encodeURIComponent(id)}/dispensacion`);
        printTable(r);
        await pressEnter();
      } else if (op === '0') return;
    } catch (e) {
      console.error('❌', e.message);
      await pressEnter();
    }
  }
}

// ===================== REPORTES =====================
async function reportesMenu() {
  while (true) {
    console.clear();
    console.log('=== REPORTES ===');
    console.log('1) Recetas resumen (/api/reportes/recetas/resumen)');
    console.log('2) Dispensación resumen (/api/reportes/dispensacion/resumen)');
    console.log('0) Volver');
    const op = await prompt('\nOpción: ');
    try {
      if (op === '1') {
        const desde = await prompt('desde YYYY-MM-DD (opcional): ');
        const hasta = await prompt('hasta YYYY-MM-DD (opcional): ');
        const doctor = await prompt('codigo_doctor (opcional): ');
        const farmacia = await prompt('codigo_farmacia (opcional): ');
        const q = `/api/reportes/recetas/resumen?desde=${encodeURIComponent(desde||'')}&hasta=${encodeURIComponent(hasta||'')}&doctor=${encodeURIComponent(doctor||'')}&farmacia=${encodeURIComponent(farmacia||'')}`;
        const r = await api('GET', q);
        console.log(JSON.stringify(r, null, 2));
        await pressEnter();
      } else if (op === '2') {
        const desde = await prompt('desde YYYY-MM-DD (opcional): ');
        const hasta = await prompt('hasta YYYY-MM-DD (opcional): ');
        const doctor = await prompt('codigo_doctor (opcional): ');
        const farmacia = await prompt('codigo_farmacia (opcional): ');
        const q = `/api/reportes/dispensacion/resumen?desde=${encodeURIComponent(desde||'')}&hasta=${encodeURIComponent(hasta||'')}&doctor=${encodeURIComponent(doctor||'')}&farmacia=${encodeURIComponent(farmacia||'')}`;
        const r = await api('GET', q);
        console.log(JSON.stringify(r, null, 2));
        await pressEnter();
      } else if (op === '0') return;
    } catch (e) {
      console.error('❌', e.message);
      await pressEnter();
    }
  }
}

// ===================== MENÚ PRINCIPAL =====================
async function mainMenu() {
  while (true) {
    console.clear();
    const s = loadSession();
    console.log('=== CORE CLI ===');
    console.log(`Base API: ${BASE}`);
    console.log(`Usuario: ${s.me?.nombre_usuario || '(no logueado)'}`);
    console.log('\nMENÚ:');
    console.log('1) Autenticación');
    console.log('2) Pacientes');
    console.log('3) Doctores');
    console.log('4) Recetas');
    console.log('5) Dispensación');
    console.log('6) Reportes');
    console.log('0) Salir');
    const op = await prompt('\nOpción: ');
    if (op === '1') await authMenu();
    else if (op === '2') await pacientesMenu();
    else if (op === '3') await doctoresMenu();
    else if (op === '4') await recetasMenu();
    else if (op === '5') await dispMenu();
    else if (op === '6') await reportesMenu();
    else if (op === '0') { console.log('👋'); process.exit(0); }
  }
}

// ===================== DAEMON (reemite QRs) =====================
function seconds(n){ return Math.max(1, parseInt(n||'60',10)); }
async function daemon() {
  const INTERVAL = seconds(process.env.CORE_INTERVAL_SEC || 60);
  const REISSUE_DAYS = parseInt(process.env.CORE_REISSUE_DAYS || '3', 10);
  console.log(`[${new Date().toISOString()}] Daemon iniciado BASE=${BASE} REISSUE_DAYS=${REISSUE_DAYS} INTERVAL=${INTERVAL}s`);
  const s = loadSession();
  if (!s.access_token || !s.refresh_token) {
    console.log('❌ No hay sesión. Ejecuta primero:\n   node core.js login');
    process.exit(1);
  }
  while (true) {
    const t0 = Date.now();
    try {
      const cand = await api('GET', `/api/recetas/vencen?dias=${REISSUE_DAYS}&limit=200&offset=0`);
      if (!Array.isArray(cand) || cand.length === 0) {
        console.log(`[${new Date().toISOString()}] No hay QR próximos a expirar (<${REISSUE_DAYS} días).`);
      } else {
        console.log(`[${new Date().toISOString()}] Encontradas ${cand.length} recetas con QR próximos a expirar…`);
        for (const r of cand) {
          try {
            const out = await api('POST', `/api/recetas/${encodeURIComponent(r.codigo_receta)}/qr/reemitir`, { ttl_hours: 168 });
            console.log(`  • Reemitido ${r.codigo_receta} → jti=${out.qr_jti} exp=${out.qr_exp}`);
          } catch (e) {
            console.log(`  • Error reemitiendo ${r.codigo_receta}: ${e.message}`);
          }
        }
      }
    } catch (e) {
      console.error(`[${new Date().toISOString()}] ERROR ciclo: ${e.message}`);
    }
    const dt = Math.round((Date.now() - t0)/1000);
    console.log(`[${new Date().toISOString()}] Cycle done, sleeping ${INTERVAL}s…`);
    await new Promise(res => setTimeout(res, Math.max(1, INTERVAL - dt) * 1000));
  }
}

// ===================== ENTRY =====================
(async () => {
  const cmd = (process.argv[2] || 'menu').toLowerCase();
  try {
    if (cmd === 'login') {
      await doLoginInteractive();
    } else if (cmd === 'daemon') {
      await daemon();
    } else {
      await mainMenu();
    }
  } catch (e) {
    console.error('❌', e.message);
    process.exit(1);
  }
})();
