# FarmaControl — v4

Sistema de inventario y facturación para droguerías. Node.js + WebSocket + PostgreSQL.

## Despliegue en producción (Railway) — PASO CRÍTICO: agregar PostgreSQL

Este sistema NECESITA una base de datos Postgres para no perder información. Sin ella,
los datos se guardan en un archivo temporal que Railway/Render puede borrar en cualquier
redeploy o reinicio.

1. En tu proyecto de Railway → clic en **+ New** → **Database** → **Add PostgreSQL**
2. Railway crea la base de datos y ya deja disponible la variable `DATABASE_URL`
   automáticamente conectada a tu servicio (no necesitas copiarla a mano si están
   en el mismo proyecto — Railway la inyecta sola).
3. Verifica en tu servicio **Farmacontrol → Variables** que aparezca `DATABASE_URL`.
   Si no aparece automáticamente, cópiala desde el servicio Postgres → Variables → `DATABASE_URL`
   y pégala como variable en el servicio de la app.
4. Redeploy el servicio. En los logs deberías ver `[DB-PG] Base de datos nueva — datos demo sembrados`
   o `[DB-PG] Cargado: X productos, Y ventas`.
5. Si en los logs ves `[AVISO] DATABASE_URL no configurada` significa que la variable
   no está llegando al servicio — revísala en Variables.

## Variables de entorno por cliente (una configuración por droguería)

| Variable | Para qué sirve | Ejemplo |
|---|---|---|
| `FC_NOMBRE` | Nombre que ve el cliente en login y topbar | `Droguería El Carmen` |
| `FC_USER1` / `FC_PASS1` | Usuario administrador | `admin` / `Clave2026!` |
| `FC_USER2` / `FC_PASS2` | Usuario operador/cajero | `cajero` / `Caja2026!` |
| `DATABASE_URL` | Conexión a Postgres (la genera Railway) | — |

Cambia `FC_PASS1` y `FC_PASS2` para cada cliente — nunca dejes las contraseñas por defecto
en un sistema que ya está en producción.

## Uso en red local (sin internet, sin Postgres)

1. Doble clic en `INICIAR.bat`
2. PC: http://localhost:3000
3. Zebra: http://<IP-del-PC>:3000

En este modo, sin `DATABASE_URL`, los datos se guardan en `data/db.json` en el PC.
Sirve como respaldo o para droguerías sin internet estable, pero no es el modo
recomendado para el servicio en la nube.

## Logo del cliente

Coloca el archivo `logo.png` del cliente en `public/fondo/logo.png` (reemplaza el anterior).

## Escáner Zebra MC9300

Funciona en dos módulos:
- **Escanear** — para ingresar mercancía nueva o sumar stock
- **Ventas** — activa "🔫 Activar escáner" y cada disparo agrega el producto al carrito
  automáticamente, sincronizado en tiempo real a cualquier otro dispositivo conectado (ej. PC de caja).
