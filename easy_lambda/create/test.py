import json
from pprint import pprint
import app

def test():
    result = app.main({})


    print("\nTest Result:")
    pprint(result)
    assert result["statusCode"] == 200


    with open("test-result.json", "w", encoding="utf-8") as fp:
        fp.write(json.dumps(result, ensure_ascii=False, default=str, indent=4))

if __name__ == "__main__":
    test()
