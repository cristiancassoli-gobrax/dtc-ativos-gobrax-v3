# Deploy no Cloudflare Pages

Este projeto ja esta pronto para publicar no Cloudflare Pages usando a pasta `dist/`.

## Estrutura

- arquivo publico: `dist/index.html`
- sem backend
- sem build obrigatorio

## Opcao mais simples

1. Suba este projeto para um repositorio Git.
2. No Cloudflare Pages, clique em `Create a project`.
3. Conecte o repositorio.
4. Preencha:
   - `Framework preset`: `None`
   - `Build command`: deixe vazio
   - `Build output directory`: `dist`
5. Clique em `Save and Deploy`.

## Se quiser automatizar a atualizacao do dist

Sempre que ajustar o painel standalone, rode:

```bash
python3 scripts/build_standalone_dashboard.py
cp dashboard-standalone.html dist/index.html
```

## Observacao

Como o painel esta em um unico `index.html`, o deploy fica bem simples e nao depende de processo de build em nuvem.
