# Frontend placeholder (Vite/TypeScript)

El dashboard operativo se sirve hoy con **Streamlit** (`app.py` y `pages/`). Este
directorio guarda un esqueleto Vite/TypeScript que no está en uso activo.

Si necesitas levantarlo:

```bash
cd frontend
npm install
npm run dev
```

Nota: no existe código React productivo; el entrypoint (`index.tsx`) es un
placeholder. Mantén esta carpeta separada del ETL/Streamlit para evitar confusión
de dependencias.
