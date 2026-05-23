#!/usr/bin/env python3
# orchestrator.py - генерация всего проекта из project_plan.json
# Поддерживает:
#   - чтение плана (JSON)
#   - генерацию каждого файла через API
#   - синтаксическую проверку JS (node --check)
#   - саморефлексию (до 5 попыток)
#   - сохранение по путям, создание папок
#   - codevet проверку и исправление (fix_with_codevet)

import os
import sys
import subprocess
import json
import time

# ------------------- НАСТРОЙКИ -------------------
OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY")
API_MODEL = "openrouter/free"
LOCAL_MODEL = "llama3.1:8b"          # изменено на вашу модель
PLAN_FILE = "project_plan.json"
OUTPUT_DIR = "."                     # корень проекта

# ------------------- ФУНКЦИИ -------------------
def generate_with_api(prompt, model=API_MODEL):
    """Генерация через OpenRouter (free)."""
    print(f"🌐 Генерация через API ({model})...")
    try:
        import requests
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENROUTER_KEY}"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}]},
            timeout=180
        )
        if resp.status_code == 200:
            code = resp.json()['choices'][0]['message']['content']
            print("✅ API вернул код.")
            return code
        else:
            print(f"⚠️ Ошибка API: {resp.status_code} — {resp.text[:200]}")
    except Exception as e:
        print(f"❌ Сбой API: {e}")
    return None

def generate_with_local(prompt):
    """Резервная генерация через локальную 7B/8B."""
    print("🖥️ Пробуем локальную модель...")
    try:
        result = subprocess.run(
            ["ollama", "run", LOCAL_MODEL, prompt],
            capture_output=True, text=True, timeout=180
        )
        if result.returncode == 0:
            print("✅ Локальная модель вернула код.")
            return result.stdout.strip()
        else:
            print(f"⚠️ Ошибка локальной модели: {result.stderr}")
    except Exception as e:
        print(f"❌ Не удалось запустить локальную модель: {e}")
    return None

def reflect_and_improve(code, error_info, file_path=""):
    """Отправляет код + ошибки в API для исправления."""
    print(f"🧠 Рефлексия для {file_path if file_path else 'файла'}...")
    prompt = f"Файл {file_path} содержит ошибки:\n{error_info}\n\nВот текущий код:\n```\n{code}\n```\n\nИсправь все ошибки и выдай полный исправленный код. Не используй сокращения '// ... rest of the code'. Верни только код."
    return generate_with_api(prompt)

def fix_with_codevet(file_path):
    """Codevet проверяет и исправляет код (использует локальную модель, если доступна)."""
    print("🩺 Codevet проверяет и автоисправляет...")
    try:
        result = subprocess.run(["codevet", "fix", file_path], capture_output=True, text=True, timeout=120)
        if "Error" not in result.stdout and "FAIL" not in result.stdout:
            print("✅ Codevet успешно завершил проверку.")
            return True, ""
        else:
            print(f"⚠️ Codevet нашёл ошибки:\n{result.stdout}")
            return False, result.stdout
    except FileNotFoundError:
        print("⚠️ Codevet не установлен (пропускаем)")
        return True, ""
    except Exception as e:
        print(f"❌ Ошибка при запуске Codevet: {e}")
        return False, str(e)

def syntax_check_js(file_path):
    """Проверяет синтаксис JavaScript файла через node --check."""
    result = subprocess.run(["node", "--check", file_path], capture_output=True, text=True)
    if result.returncode == 0:
        return True, ""
    else:
        return False, result.stderr

def generate_file(file_info, retry_limit=5):
    """Генерирует один файл: вызывает API, проверяет синтаксис, рефлексия."""
    path = file_info["path"]
    required = file_info.get("required", True)
    if not required:
        print(f"⏭️ {path} помечен как необязательный, пропускаем")
        return True

    full_path = os.path.join(OUTPUT_DIR, path)
    if os.path.exists(full_path):
        print(f"⏩ {path} уже существует, пропускаем")
        return True

    os.makedirs(os.path.dirname(full_path), exist_ok=True)

    lines = file_info.get("lines", 200)
    description = file_info.get("description", "")
    is_js = path.endswith(".js")

    prompt = f"""Ты — эксперт Node.js. Напиши полный рабочий код для файла {path}.

Описание: {description}
Примерный объём: {lines} строк.

Требования:
- Полный код без сокращений (запрещены комментарии '// ... rest of the code').
- Используй CommonJS (require) для модулей.
- Обрабатывай ошибки, логируй через core/logger.js.
- Экспортируй функции/классы.
- Код должен быть готов к запуску.

Выдай только код, без пояснений.
"""

    for attempt in range(retry_limit):
        print(f"🔄 Генерация {path} (попытка {attempt+1}/{retry_limit})...")
        code = generate_with_api(prompt)
        if not code:
            time.sleep(3)
            continue

        temp_path = full_path + ".tmp"
        with open(temp_path, "w", encoding="utf-8") as f:
            f.write(code)

        syntax_ok = True
        error_msg = ""
        if is_js:
            syntax_ok, error_msg = syntax_check_js(temp_path)
            if not syntax_ok:
                print(f"⚠️ Синтаксическая ошибка в {path}:\n{error_msg[:300]}")
                fixed = reflect_and_improve(code, error_msg, path)
                if fixed:
                    with open(temp_path, "w", encoding="utf-8") as f:
                        f.write(fixed)
                    syntax_ok, error_msg = syntax_check_js(temp_path)
                    if syntax_ok:
                        code = fixed
                    else:
                        print(f"❌ После рефлексии ошибки остались: {error_msg[:200]}")
                else:
                    print("❌ Рефлексия не дала результата")
                    syntax_ok = False

        if syntax_ok:
            os.rename(temp_path, full_path)
            print(f"✅ {path} сгенерирован и прошёл проверку")
            return True
        else:
            os.remove(temp_path)
            prompt = f"""Файл {path} не прошёл проверку. Ошибка:
{error_msg}

Требования к файлу:
{description}

Напиши полный исправленный код. Без сокращений.
"""
            time.sleep(2)

    print(f"❌ Не удалось сгенерировать {path} после {retry_limit} попыток")
    return False

def main():
    if not OPENROUTER_KEY:
        print("❌ Установите переменную окружения OPENROUTER_API_KEY")
        sys.exit(1)

    if len(sys.argv) > 1 and sys.argv[1] != "--plan":
        prompt = " ".join(sys.argv[1:])
        print("📝 Режим одного файла (совместимость)")
        code = generate_with_api(prompt)
        if code:
            with open("generated_code.py", "w") as f:
                f.write(code)
            print("💾 Сохранено в generated_code.py")
        return

    if not os.path.exists(PLAN_FILE):
        print(f"❌ Файл {PLAN_FILE} не найден. Запусти с аргументом-промтом или создай план.")
        sys.exit(1)

    with open(PLAN_FILE, "r", encoding="utf-8") as f:
        plan = json.load(f)

    files = plan.get("files", [])
    required_files = [f for f in files if f.get("required", True)]
    total = len(required_files)
    print(f"📋 Найдено {total} обязательных файлов для генерации")

    success_count = 0
    for idx, file_info in enumerate(required_files, 1):
        print(f"\n🔨 [{idx}/{total}] Обработка {file_info['path']}")
        if generate_file(file_info, retry_limit=5):
            success_count += 1
        time.sleep(1)

    print(f"\n🏁 Итог: успешно сгенерировано {success_count} из {total} файлов")
    if success_count < total:
        print("⚠️ Некоторые файлы не созданы. Проверьте логи выше.")

if __name__ == "__main__":
    try:
        import requests
    except ImportError:
        print("❌ Установите requests: pip install requests")
        sys.exit(1)
    main()
