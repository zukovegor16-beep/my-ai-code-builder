#!/usr/bin/env python3
import os
import sys
import json
import subprocess
import time
import shutil
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from threading import Event

from orchestrator import generate_with_api, reflect_and_improve, generate_with_local, syntax_check_js

# ------------------- КОНФИГУРАЦИЯ -------------------
PLAN_FILE = "project_plan.json"
SOURCE_DIR = "краулер"
PROJECT_ROOT = "."               # если нужно генерировать в "краулер", смените на "краулер"
MAX_RETRIES_PER_FILE = 3
GLOBAL_CYCLES = 3
DRY_RUN = False
USE_LOCAL = False                # включает параллельную гонку локальной и API
MAX_PARALLEL_FILES = 3           # сколько файлов генерировать одновременно

# ------------------- УМНАЯ РЕФЛЕКСИЯ (FALLBACK) -------------------
def reflect_with_fallback(code, error_info):
    """Локальная рефлексия, при неудаче – API."""
    if USE_LOCAL:
        print("🧠 Локальная рефлексия...")
        prompt = (
            f"Файл содержит ошибки:\n{error_info}\n\n"
            f"Вот текущий код:\n```\n{code}\n```\n\n"
            f"Исправь все ошибки и выдай полный исправленный код. "
            f"Не используй сокращения '// ... rest of the code'. Верни только код."
        )
        fixed = generate_with_local(prompt)
        if fixed and fixed.strip():
            return fixed
        print("⚠️ Локальная рефлексия не удалась, пробуем API...")
    return reflect_and_improve(code, error_info)

# ------------------- ГОНКА ГЕНЕРАЦИЙ -------------------
def race_generation(prompt, use_local):
    """
    Запускает локальную и API генерацию параллельно.
    Возвращает ответ от того, кто первым вернёт непустой результат.
    """
    result = [None]
    stop_event = Event()

    def local_worker():
        if not use_local:
            return
        res = generate_with_local(prompt)
        if res and not stop_event.is_set():
            result[0] = res
            stop_event.set()

    def api_worker():
        res = generate_with_api(prompt)
        if res and not stop_event.is_set():
            result[0] = res
            stop_event.set()

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        if use_local:
            futures.append(executor.submit(local_worker))
        futures.append(executor.submit(api_worker))
        # Ждём завершения любого
        done, not_done = wait(futures, return_when=FIRST_COMPLETED)
        stop_event.set()          # сигнал остановить оставшийся поток
        for f in not_done:
            f.cancel()
    return result[0]

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
    if file_path.endswith(".js"):
        result = subprocess.run(["node", "--check", file_path], capture_output=True, text=True)
        if result.returncode != 0:
            return False, f"syntax error: {result.stderr[:200]}"
    return True, "ok"

def generate_or_fix_file(file_info):
    path = file_info["path"]
    full_path = os.path.join(PROJECT_ROOT, path)
    description = file_info.get("description", "")
    lines = file_info.get("lines", 200)
    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    prompt = f"""Файл: {path}
Описание: {description}
Ожидаемый объём: примерно {lines} строк.

Напиши полный рабочий код. Требования:
- Полный код, без сокращений (запрещены комментарии '// ... rest of the code').
- Используй CommonJS (require).
- Обрабатывай ошибки, логируй через core/logger.js.
- Экспортируй функции/классы.
- Код должен быть готов к запуску.
Выдай только код, без пояснений.
"""
    for attempt in range(MAX_RETRIES_PER_FILE):
        print(f"🔄 Генерация/исправление {path} (попытка {attempt+1})")

        # Гонка: локальная vs API (оба потока параллельно)
        code = race_generation(prompt, USE_LOCAL)

        if not code or code.strip() == "":
            time.sleep(2)
            continue

        temp_path = full_path + ".tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(code)

        # Синтаксическая проверка JS
        if path.endswith(".js"):
            result = subprocess.run(["node", "--check", temp_path], capture_output=True, text=True)
            if result.returncode != 0:
                print(f"⚠️ Синтаксическая ошибка: {result.stderr[:200]}")
                fixed = reflect_with_fallback(code, result.stderr)
                if fixed:
                    with open(temp_path, "w", encoding="utf-8") as f:
                        f.write(fixed)
                    result2 = subprocess.run(["node", "--check", temp_path], capture_output=True, text=True)
                    if result2.returncode == 0:
                        code = fixed
                    else:
                        print(f"❌ После рефлексии ошибки остались: {result2.stderr[:200]}")
                        os.remove(temp_path)
                        continue
                else:
                    os.remove(temp_path)
                    continue

        shutil.move(temp_path, full_path)
        print(f"✅ {path} готов")
        return True
    return False

def process_file(file_info):
    """Обёртка для параллельного запуска."""
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

    # ---- 2. Генерация недостающих или неполных файлов параллельно ----
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

    # ---- 3. Глобальная синтаксическая проверка и исправление ----
    print("\n🔍 Глобальная проверка всех JS файлов...")
    js_files = []
    for root, _, files in os.walk(PROJECT_ROOT):
        for fname in files:
            if fname.endswith(".js"):
                js_files.append(os.path.join(root, fname))
    print(f"Найдено {len(js_files)} JS файлов")

    for cycle in range(GLOBAL_CYCLES):
        errors = {}
        for js_file in js_files:
            result = subprocess.run(["node", "--check", js_file], capture_output=True, text=True)
            if result.returncode != 0:
                errors[js_file] = result.stderr
        if not errors:
            print(f"✅ Цикл {cycle+1}: все файлы прошли синтаксис")
            break
        print(f"⚠️ Цикл {cycle+1}: найдено {len(errors)} файлов с ошибками")
        for js_file, err_msg in errors.items():
            rel_path = os.path.relpath(js_file, PROJECT_ROOT)
            with open(js_file, "r", encoding="utf-8") as f:
                code = f.read()
            fixed = reflect_with_fallback(code, err_msg)
            if fixed:
                with open(js_file, "w", encoding="utf-8") as f:
                    f.write(fixed)
                print(f"   Исправлен {rel_path}")
            else:
                print(f"   ❌ Не удалось исправить {rel_path}")
        time.sleep(1)
    else:
        print("⚠️ После всех циклов остались ошибки, требуется ручная доработка")

    print("\n🏁 Готово. Запустите проект:")
    print("   docker-compose up -d")
    print("   npm run migrate")
    print("   npm start")

if __name__ == "__main__":
    if "--dry-run" in sys.argv:
        DRY_RUN = True
        print("🏁 Режим DRY-RUN")
    if "--local" in sys.argv:
        USE_LOCAL = True
        print("🏁 Режим: гонка локальной модели и API")
    main()
