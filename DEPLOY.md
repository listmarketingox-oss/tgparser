# 🚀 Деплой TGParse PRO на Railway

## Шаг 1 — GitHub

1. Зайди на https://github.com → New repository → `tgparse-bot`
2. Загрузи файлы: `bot.py`, `requirements.txt`, `Procfile`

```bash
cd ~/Downloads
git init
git add bot.py requirements.txt Procfile
git commit -m "TGParse PRO"
git remote add origin https://github.com/ТВОй_ЮЗЕР/tgparse-bot.git
git push -u origin main
```

## Шаг 2 — Railway

1. https://railway.app → Login with GitHub
2. New Project → Deploy from GitHub repo → выбери `tgparse-bot`
3. Railway сам найдёт Procfile и запустит бота

## Шаг 3 — Переменные окружения

Railway → Variables → добавь:
```
TG_BOT_TOKEN=твой_токен
TG_API_ID=твой_api_id
TG_API_HASH=твой_api_hash
ADMIN_ID=твой_telegram_id
```

## Тарифы бота

| Тариф | Цена | Сообщений | Чатов | Расписание |
|-------|------|-----------|-------|------------|
| Free  | 0    | 100       | 1     | ❌         |
| Basic | 50 ⭐ (~$1) | 2,000 | 3 | ❌     |
| Pro   | 150 ⭐ (~$3) | 10,000 | 10 | ✅   |

## Вывод денег
Stars → TON/рубли через https://fragment.com

## Целевая аудитория
- Маркетологи и SMM специалисты
- HR и рекрутеры  
- Исследователи рынка
- Владельцы бизнеса

## Где рекламировать
- @freelance_ru, @smm_rus, @digital_market
- Telemetr.me — реклама в тематических каналах
