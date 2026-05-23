#!/usr/bin/env python3
import os
import sys
import json
import subprocess
import time
import shutil
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# ------------------- КОНФИГУРАЦИЯ -------------------
PLAN_FILE = "project_plan.json"
SOURCE_DIR = "краулер"
PROJECT_ROOT = "."
MAX_RETRIES_PER_FILE = 2          # уменьшено, т.к. рефлексия стала умнее
GLOBAL_CYCLES = 1                # одного глобального цикла достаточно
DRY_RUN = False
MAX_PARALLEL_FILES = 3

# ------------------- DeepSeek API -------------------
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "sk-f28bcbb45dca4f5b9db77b01b396625f")
API_URL = "https://api.deepseek.com/v1/chat/completions"
HEADERS = {
    "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
    "Content-Type": "application/json"
}

# Системный промпт для первичной генерации – строгий и без лишних слов
SYSTEM_PROMPT = (
    "Ты — эксперт-разработчик. Пиши безупречный JavaScript (CommonJS) код. "
    "Логируй ошибки через core/logger. Всегда используй try/catch. "
    "Не добавляй комментариев, не оборачивай код в markdown. "
    "Возвращай только код, готовый к запуску."
)

# Системный промпт для исправления ошибок
FIX_SYSTEM_PROMPT = (
    "Ты — эксперт по отладке. Исправь синтаксическую ошибку в предоставленном коде, "
    "сохранив всю функциональность. Отвечай только исправленным кодом без пояснений и без markdown-обёртки."
)

def extract_code(raw_response):
    """Удаляет markdown-обёртку ```javascript ... ```, если она есть."""
    if not raw_response:
        return ""
    # Ищем блок кода в формате ```javascript ... ```
    match = re.search(r"```(?:javascript|js)?\s*\n?(.*?)\n?```", raw_response, re.DOTALL)
    if match:
        return match.group(1).strip()
    # Если нет обёртки, возвращаем как есть, но убираем возможные начальные/конечные ```
    return raw_response.strip().strip("`")

def generate_with_deepseek(prompt, max_tokens=1200):
    """Вызов DeepSeek API. Возвращает очищенный код."""
    payload = {
        "model": "deepseek-chat",
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": prompt}
        ],
        "temperature": 0.2,
        "max_tokens": max_tokens,
        "stream": False
    }
    try:
        resp = requests.post(API_URL, headers=HEADERS, json=payload, timeout=120)
        if resp.status_code == 200:
            data = resp.json()
            raw = data["choices"][0]["message"]["content"]
            return extract_code(raw)
        else:
            print(f"⚠️ Ошибка API: {resp.status_code} — {resp.text[:200]}")
            return None
    except Exception as e:
        print(f"❌ Сбой API: {e}")
        return None

def reflect_and_improve(code, error_info):
    """Исправляет ошибки в коде через DeepSeek."""
    prompt = (
        f"Код содержит ошибку:\n{error_info}\n\n"
        f"Вот сам код:\n```javascript\n{code}\n```\n\n"
        "Исправь ошибку и верни полный исправленный код."
    )
    return generate_with_deepseek(prompt, max_tokens=2000)  # для рефлексии можно чуть больше токенов

# ------------------- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ -------------------
def load_plan():
    with open(PLAN_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def find_file_in_source(target_filename, source_root):
    for root, _, files in os.walk(source_root):
        if target_filename in files:
            return os.path.join(root, target_filename)
    return None

def is_file_complete(file_path):
    if not os.path.exists(file_path):
        return False, "missing"
    if os.path.getsize(file_path) < 50:
        return False, "too_small"
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
    incomplete_markers = ["// ... rest", "// ... existing", "// TODO: complete", "FIXME", "..."]
    for marker in incomplete_markers:
        if marker in content:
            return False, f"contains '{marker}'"
    # Синтаксическую проверку здесь не делаем – она будет при генерации и в глобальной фазе
    return True, "ok"

def generate_or_fix_file(file_info):
    path = file_info["path"]
    full_path = os.path.join(PROJECT_ROOT, path)
    description = file_info.get("description", "")
    lines = file_info.get("lines", 150)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    # Динамический лимит токенов в зависимости от ожидаемого размера файла
    if lines < 150:
        max_tok = 800
    elif lines < 350:
        max_tok = 1500
    else:
        max_tok = 2000

    # Лаконичный промпт
    prompt = (
        f"Создай файл {path}.\n"
        f"Описание: {description}\n\n"
        "Требования: CommonJS, логирование через core/logger, экспорт функций, обработка ошибок."
    )

    for attempt in range(MAX_RETRIES_PER_FILE):
        print(f"🔄 Генерация {path} (попытка {attempt+1})")
        code = generate_with_deepseek(prompt, max_tokens=max_tok)
        if not code:
            time.sleep(2)
            continue

        with open(full_path, "w", encoding="utf-8") as f:
            f.write(code)

        # Проверка синтаксиса JS
        if path.endswith(".js"):
            result = subprocess.run(["node", "--check", full_path], capture_output=True, text=True)
            if result.returncode != 0:
                print(f"⚠️ Ошибка синтаксиса в {path}: {result.stderr[:200]}")
                fixed = reflect_and_improve(code, result.stderr)
                if fixed:
                    with open(full_path, "w", encoding="utf-8") as f:
                        f.write(fixed)
                    # Перепроверка
                    result2 = subprocess.run(["node", "--check", full_path], capture_output=True, text=True)
                    if result2.returncode == 0:
                        print(f"✅ {path} исправлен")
                        return True
                    else:
                        print(f"❌ Не удалось исправить {path}: {result2.stderr[:200]}")
                        # Удаляем битый файл, если последняя попытка
                        if attempt == MAX_RETRIES_PER_FILE - 1:
                            os.remove(full_path)
                        continue
                else:
                    if attempt == MAX_RETRIES_PER_FILE - 1:
                        os.remove(full_path)
                    continue
            else:
                print(f"✅ {path} готов")
                return True
        else:
            # Не JS — просто считаем готовым
            print(f"✅ {path} создан")
            return True
    return False

def process_file(file_info):
    path = file_info["path"]
    success = generate_or_fix_file(file_info)
    return path, success

def main():
    if not os.path.exists(PLAN_FILE):
        print(f"❌ Файл {PLAN_FILE} не найден")
        sys.exit(1)

    plan = load_plan()
    required_files = [f for f in plan["files"] if f.get("required", True)]
    print(f"📋 План содержит {len(required_files)} обязательных файлов")

    # ---- 1. Импорт существующих файлов из SOURCE_DIR ----
    if os.path.exists(SOURCE_DIR):
        print(f"\n📂 Сканирование исходной папки: {SOURCE_DIR}")
        for file_info in required_files:
            target_path = file_info["path"]
            target_name = os.path.basename(target_path)
            full_target = os.path.join(PROJECT_ROOT, target_path)
            if os.path.exists(full_target):
                continue
            src = find_file_in_source(target_name, SOURCE_DIR)
            if src:
                print(f"📥 Найден существующий файл: {src} -> {target_path}")
                if not DRY_RUN:
                    os.makedirs(os.path.dirname(full_target), exist_ok=True)
                    shutil.copy2(src, full_target)
                else:
                    print(f"   [dry-run] скопировать {src} в {target_path}")

    # ---- 2. Генерация недостающих файлов ----
    print("\n🔧 Проверка полноты и генерация отсутствующих...")
    incomplete_files = []
    for idx, file_info in enumerate(required_files, 1):
        path = file_info["path"]
        full_path = os.path.join(PROJECT_ROOT, path)
        complete, reason = is_file_complete(full_path)
        if complete:
            print(f"✅ [{idx}/{len(required_files)}] {path} существует и полный")
        else:
            print(f"⚠️ [{idx}/{len(required_files)}] {path} отсутствует или неполный ({reason})")
            incomplete_files.append(file_info)

    if incomplete_files:
        print(f"\n⚡ Запуск параллельной генерации {len(incomplete_files)} файлов (воркеров: {MAX_PARALLEL_FILES})...")
        with ThreadPoolExecutor(max_workers=MAX_PARALLEL_FILES) as executor:
            futures = {executor.submit(process_file, fi): fi["path"] for fi in incomplete_files}
            for future in as_completed(futures):
                path, success = future.result()
                if success:
                    print(f"   ✓ {path} создан/исправлен")
                else:
                    print(f"   ✗ {path} не удалось после {MAX_RETRIES_PER_FILE} попыток")
    else:
        print("🎉 Все файлы уже полны!")

    # ---- 3. Однократная глобальная синтаксическая проверка ----
    print("\n🔍 Глобальная проверка всех JS файлов...")
    js_files = []
    for root, _, files in os.walk(PROJECT_ROOT):
        for fname in files:
            if fname.endswith(".js"):
                js_files.append(os.path.join(root, fname))
    print(f"Найдено {len(js_files)} JS файлов")

    errors = {}
    for js_file in js_files:
        result = subprocess.run(["node", "--check", js_file], capture_output=True, text=True)
        if result.returncode != 0:
            errors[js_file] = result.stderr
    if errors:
        print(f"⚠️ Найдено {len(errors)} файлов с синтаксическими ошибками. Попытка исправления...")
        for js_file, err_msg in errors.items():
            rel_path = os.path.relpath(js_file, PROJECT_ROOT)
            with open(js_file, "r", encoding="utf-8") as f:
                code = f.read()
            fixed = reflect_and_improve(code, err_msg)
            if fixed:
                with open(js_file, "w", encoding="utf-8") as f:
                    f.write(fixed)
                print(f"   Исправлен {rel_path}")
            else:
                print(f"   ❌ Не удалось исправить {rel_path}")
    else:
        print("✅ Все JS-файлы прошли синтаксическую проверку!")

    print("\n🏁 Готово. Запустите проект:")
    print("   docker-compose up -d")
    print("   npm run migrate")
    print("   npm start")

if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        DRY_RUN = True
        print("🏁 Режим DRY-RUN")
    main()
