import os
import sys
import subprocess
import json

# ------------------- НАСТРОЙКИ -------------------
OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY")   # ваш ключ
API_MODEL = "openrouter/free"                           # бесплатная маршрутизация
LOCAL_MODEL = "qwen2.5-coder:7b"                        # локальная 7B (если нужна)
OUTPUT_FILE = "generated_code.py"                       # итоговый файл

# ------------------- ФУНКЦИИ -------------------
def generate_with_api(prompt, model=API_MODEL):
    """Генерация через OpenRouter (free)."""
    print(f"🌐 Генерация через API ({model})...")
    try:
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENROUTER_KEY}"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}]},
            timeout=120
        )
        if resp.status_code == 200:
            code = resp.json()['choices'][0]['message']['content']
            print("✅ API вернул код.")
            return code
        else:
            print(f"⚠️ Ошибка API: {resp.status_code} — {resp.text}")
    except Exception as e:
        print(f"❌ Сбой API: {e}")
    return None

def generate_with_local(prompt):
    """Резервная генерация через локальную 7B."""
    print("🖥️ Пробуем локальную 7B...")
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

def reflect_and_improve(code, error_info=""):
    """Отправляем код в API на доработку с учётом ошибок."""
    print("🧠 Отправляем код на доработку (рефлексия)...")
    prompt = f"Вот код Python:\n```python\n{code}\n```\n"
    if error_info:
        prompt += f"Найденные ошибки или замечания:\n{error_info}\n"
    prompt += "Исправь все ошибки, улучши структуру и выдай полный исправленный код. Без лишних пояснений, только код."
    return generate_with_api(prompt)

# ------------------- ГЛАВНЫЙ ЦИКЛ -------------------
def main():
    # --- Приоритет загрузки промта ---
    # 1. Если есть файл prompt.txt – читаем его полностью
    if os.path.exists("prompt.txt"):
        with open("prompt.txt", "r", encoding="utf-8") as f:
            prompt = f.read()
        print("📄 Промт загружен из файла prompt.txt")
    # 2. Иначе – из аргументов командной строки
    elif len(sys.argv) > 1:
        prompt = " ".join(sys.argv[1:])
        print("📝 Промт взят из аргументов командной строки")
    # 3. Иначе – стандартный промт
    else:
        prompt = """Напиши модуль на Python `proxy_manager.py`, который:
- содержит класс ProxyManager с асинхронными методами,
- умеет загружать список прокси из списка,
- имеет метод get_proxy() для получения случайного прокси,
- имеет метод rotate_proxy() для удаления текущего прокси и перехода к следующему.
Используй библиотеку asyncio."""
        print("⚠️ Промт не передан, используется стандартный.")

    print(f"📝 Длина промта: {len(prompt)} символов, начало: {prompt[:150]}...")

    # --- Шаг 1: Генерация кода (сначала API) ---
    code = generate_with_api(prompt)
    if not code:                     # если API недоступен, пробуем локальную
        code = generate_with_local(prompt)
    if not code:
        print("💔 Не удалось сгенерировать код. Проверьте подключение и настройки.")
        return

    # Сохраняем черновик
    with open(OUTPUT_FILE, "w") as f:
        f.write(code)
    print(f"💾 Черновик сохранён в {OUTPUT_FILE}")

    # --- Шаг 2: Проверка Codevet ---
    success, errors = fix_with_codevet(OUTPUT_FILE)

    # --- Шаг 3: Если ошибки есть, дорабатываем через API ---
    if not success:
        print("🔄 Codevet нашёл проблемы. Отправляем на доработку...")
        improved = reflect_and_improve(code, errors)
        if improved:
            with open(OUTPUT_FILE, "w") as f:
                f.write(improved)
            print("✅ Доработанная версия сохранена.")
            # Повторная проверка
            fix_with_codevet(OUTPUT_FILE)
        else:
            print("⚠️ Не удалось получить доработанный код.")
    else:
        print("✨ Код прошёл первичную проверку. Можно запускать.")

    print(f"🏁 Итоговый код сохранён в {OUTPUT_FILE}")

if __name__ == "__main__":
    # Импортируем requests внутри, чтобы не было ошибки, если модуль не установлен
    global requests
    try:
        import requests
    except ImportError:
        print("❌ Установите requests: pip install requests")
        sys.exit(1)
    main()
