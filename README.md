# DTC Ativos x Frota Gobrax V3

Dashboard estatico pronto para publicacao no Cloudflare Pages.

## Estrutura principal

- `dist/index.html`: arquivo publico usado no deploy
- `dashboard-concept.html`: fonte principal do layout
- `scripts/generate_dashboard_data.py`: gera os dados consolidados
- `scripts/build_standalone_dashboard.py`: gera a versao standalone

## Publicacao no Cloudflare Pages

- `Framework preset`: `None`
- `Build command`: deixar vazio
- `Build output directory`: `dist`

## Atualizacao do painel

Se voce alterar o dashboard e quiser atualizar a pasta de deploy:

```bash
python3 scripts/generate_dashboard_data.py
python3 scripts/build_standalone_dashboard.py
cp dashboard-standalone.html dist/index.html
```
