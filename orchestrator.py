import os
import subprocess
import requests

# ------------------- НАСТРОЙКИ -------------------
OPENROUTER_KEY = os.environ.get("OPENROUTER_API_KEY")   # ваш ключ из секретов Codespaces
API_MODEL = "qwen/qwen-2.5-coder-32b-instruct:free"     # бесплатная 32B через API
LOCAL_MODEL = "qwen2.5-coder:7b"                        # ваша локальная 7B
OUTPUT_FILE = "proxy_manager.py"                        # файл, который создаём

# ------------------- ФУНКЦИИ -------------------
def generate_with_api(prompt, model=API_MODEL):
    """Генерация через OpenRouter (обычно 32B)."""
    print(f"🌐 Генерация через API ({model})...")
    try:
        resp = requests.post(
            "https://openrouter.ai/api/v1/chat/completions",
            headers={"Authorization": f"Bearer {OPENROUTER_KEY}"},
            json={"model": model, "messages": [{"role": "user", "content": prompt}]},
            timeout=60
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
            capture_output=True, text=True, timeout=120
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
    """Codevet проверяет и исправляет код (использует локальную модель)."""
    print("🩺 Codevet проверяет и автоисправляет...")
    result = subprocess.run(["codevet", "fix", file_path], capture_output=True, text=True)
    if "Error" not in result.stdout and "FAIL" not in result.stdout:
        print("✅ Codevet успешно завершил проверку.")
        return True, ""
    else:
        print(f"⚠️ Codevet нашёл ошибки:\n{result.stdout}")
        return False, result.stdout

def reflect_and_improve(code, error_info=""):
    """Отправляем код в 32B на доработку с учётом ошибок."""
    print("🧠 Отправляем код на доработку в 32B (рефлексия)...")
    prompt = f"Вот код Python:\n```python\n{code}\n```\n"
    if error_info:
        prompt += f"Найденные ошибки или замечания:\n{error_info}\n"
    prompt += "Исправь все ошибки, улучши структуру и выдай полный исправленный код."
    return generate_with_api(prompt)  # снова вызываем 32B

# ------------------- ГЛАВНЫЙ ЦИКЛ -------------------
def main():
    # Промпт для генерации (отредактируйте под свою задачу)
    prompt = """Напиши модуль на Python `proxy_manager.py`, который:
    - содержит класс ProxyManager с асинхронными методами,
    - умеет загружать список прокси из списка,
    - имеет метод get_proxy() для получения случайного прокси,
    - имеет метод rotate_proxy() для удаления текущего прокси и перехода к следующему.
    Используй библиотеку asyncio."""

    # --- Шаг 1: Генерация кода (всегда пытаемся через 32B) ---
    code = generate_with_api(prompt)
    if not code:  # если API недоступен, используем локальную 7B
        code = generate_with_local(prompt)
    if not code:
        print("💔 Не удалось сгенерировать код. Проверьте подключение и настройки.")
        return

    # Сохраняем черновик
    with open(OUTPUT_FILE, "w") as f:
        f.write(code)
    print(f"💾 Черновик сохранён в {OUTPUT_FILE}")

    # --- Шаг 2: Первичная проверка Codevet ---
    success, errors = fix_with_codevet(OUTPUT_FILE)

    # --- Шаг 3: Если Codevet не справился или хотим идеала — подключаем 32B для финальной шлифовки ---
    if not success:
        print("🔄 Codevet не смог исправить все ошибки. Подключаем 32B для глубокой доработки...")
        improved_code = reflect_and_improve(code, errors)
        if improved_code:
            with open(OUTPUT_FILE, "w") as f:
                f.write(improved_code)
            print("✅ 32B предложила исправленную версию.")
            # Повторно проверяем Codevet
            success2, _ = fix_with_codevet(OUTPUT_FILE)
            if success2:
                print("🎉 После доработки 32B код прошёл проверку!")
            else:
                print("⚠️ Остались ошибки. Возможно, нужно уточнить промпт или проверить код вручную.")
        else:
            print("❌ Не удалось получить доработку от 32B.")
    else:
        # Всё хорошо, но для максимального качества можно всё равно пропустить через 32B
        print("✨ Код уже хорош. Хотите финальную полировку от 32B? (y/n) ", end="")
        choice = input().strip().lower()
        if choice == 'y':
            print("Отправляю на полировку...")
            polished = reflect_and_improve(code, "Сделай код более читаемым и эффективным.")
            if polished:
                with open(OUTPUT_FILE, "w") as f:
                    f.write(polished)
                print("✅ Полировка завершена.")
                fix_with_codevet(OUTPUT_FILE)  # финальная проверка
            else:
                print("⚠️ Не удалось выполнить полировку.")
        else:
            print("Оставляем как есть.")

    print(f"🏁 Итоговый код сохранён в {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
