"""
Утилита для просмотра доступных тегов (tag_id) в Polymarket API.

Запуск:
    python -m app.list_tags
    python -m app.list_tags --limit 200
    python -m app.list_tags --search crypto
"""

import argparse
import json
import sys
from typing import Any, Dict, List

import requests


def get_tags(limit: int = 100, search: str | None = None) -> List[Dict[str, Any]]:
    """
    Получить список тегов из Gamma API.
    
    Args:
        limit: Максимальное количество тегов для получения
        search: Поиск по названию тега (необязательно)
    
    Returns:
        Список тегов с их ID и названиями
    """
    url = "https://gamma-api.polymarket.com/tags"
    params: Dict[str, Any] = {"limit": limit}
    
    if search:
        params["search"] = search
    
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        if isinstance(data, list):
            return data
        elif isinstance(data, dict):
            return data.get("data") or data.get("results") or data.get("tags") or []
        return []
    except Exception as exc:  # noqa: BLE001
        print(f"Ошибка при получении тегов: {exc}", file=sys.stderr)
        return []


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Просмотр доступных тегов (tag_id) в Polymarket API"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Максимальное количество тегов для отображения (по умолчанию: 100)",
    )
    parser.add_argument(
        "--search",
        type=str,
        help="Поиск тегов по названию (например: crypto, politics, sports)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Вывести результат в формате JSON",
    )
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("ПОЛУЧЕНИЕ СПИСКА ТЕГОВ ИЗ POLYMARKET GAMMA API")
    print("=" * 80)
    print()
    
    if args.search:
        print(f"Поиск тегов по запросу: '{args.search}'")
    else:
        print(f"Получение первых {args.limit} тегов...")
    
    print()
    
    tags = get_tags(limit=args.limit, search=args.search)
    
    if not tags:
        print("⚠ Теги не найдены или произошла ошибка при запросе.")
        return
    
    if args.json:
        print(json.dumps(tags, indent=2, ensure_ascii=False))
    else:
        print(f"Найдено тегов: {len(tags)}\n")
        print(f"{'ID':<10} {'Название':<50} {'Slug':<30}")
        print("-" * 90)
        
        for tag in tags:
            if isinstance(tag, dict):
                tag_id = tag.get("id", "N/A")
                label = tag.get("label") or tag.get("name") or tag.get("title") or "N/A"
                slug = tag.get("slug", "N/A")
                print(f"{tag_id:<10} {label[:48]:<50} {slug[:28]:<30}")
        
        print()
        print("=" * 80)
        print("ИСПОЛЬЗОВАНИЕ:")
        print("=" * 80)
        print("Чтобы использовать тег в app/app_config.py, установите:")
        print("  gamma_api_tag_id: int | None = <ID_ТЕГА>")
        print()
        print("Примеры популярных тегов:")
        popular_tags = [
            ("Crypto", "2"),
            ("Politics", "21"),
            ("Sports games", "100639"),
        ]
        for name, tag_id in popular_tags:
            found = any(str(t.get("id")) == tag_id for t in tags if isinstance(t, dict))
            if found:
                print(f"  - {name}: tag_id = {tag_id}")


if __name__ == "__main__":
    main()
