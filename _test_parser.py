"""Quick validation test for DocumentParserService"""
import sys
sys.stdout.reconfigure(encoding='utf-8')

print("=" * 60)
print("DocumentParserService 集成测试")
print("=" * 60)

# Test 1: Syntax
print("\n[1/7] 语法检查...", end=" ")
try:
    with open(r"d:\Codeing\pythonprojects\data-pipeline-childcare\多数据源获取数据.py", encoding='utf-8') as f:
        code = f.read()
    compile(code, "test.py", "exec")
    print("OK")
except SyntaxError as e:
    print(f"FAIL line {e.lineno}: {e.msg}")
    sys.exit(1)

# Test 2: Load classes (stop before main() call)
print("[2/7] 加载类定义...", end=" ")
try:
    exec_globals = {}
    lines = code.split('\n')
    cut_line = None
    for i, line in enumerate(lines):
        if line.startswith("main({") or line.startswith("main('"):
            cut_line = i
            break
    safe_code = '\n'.join(lines[:cut_line]) if cut_line else code
    exec(compile(safe_code, "test.py", "exec"), exec_globals)
    print("OK")
except Exception as e:
    print(f"FAIL: {type(e).__name__}: {e}")
    import traceback; traceback.print_exc()
    sys.exit(1)

# Test 3: Classes exist
print("[3/7] 验证类存在...", end=" ")
for cls_name in ['EmbeddedImageUploader', 'DataCleaningPipeline', 'DocumentParserService', 'SearchApiScraper']:
    assert cls_name in exec_globals, f"{cls_name} not found"
print("OK")

# Test 4: Instantiate
print("[4/7] 实例化 DocumentParserService...", end=" ")
DPS = exec_globals['DocumentParserService']
parser = DPS()
print(f"OK (MarkItDown: {'可用' if parser._markitdown else '不可用'})")

# Test 5: DataCleaningPipeline
print("[5/7] 测试清洗管道...", end=" ")
cleaner = exec_globals['DataCleaningPipeline']()
test_input = "正文内容。\n\n第 1 页\n\n分享到微信\n\n有效内容。\n\n京ICP备12345号\n\n---"
cleaned = cleaner.clean_document(test_input)
assert "正文内容" in cleaned
assert "第 1 页" not in cleaned
assert "分享到" not in cleaned
assert "ICP" not in cleaned
print(f"OK ({len(cleaned)} chars)")

# Test 6: Format routing (CSV, JSON, TXT, MD)
print("[6/7] 测试格式路由...", end=" ")
csv_r = parser.parse(b"name,age\nAlice,30\nBob,25", ".csv")
assert "Alice" in csv_r, f"CSV: {csv_r}"
json_r = parser.parse(b'{"key":"value"}', ".json")
assert "key" in json_r, f"JSON: {json_r}"
txt_r = parser.parse("纯文本测试".encode('utf-8'), ".txt")
assert "纯文本" in txt_r, f"TXT: {txt_r}"
md_r = parser.parse("# 标题\n\n**加粗**".encode('utf-8'), ".md")
assert "标题" in md_r, f"MD: {md_r}"
print("OK (csv/json/txt/md)")

# Test 7: EmbeddedImageUploader structure
print("[7/7] 验证 EmbeddedImageUploader...", end=" ")
EIU = exec_globals['EmbeddedImageUploader']
assert hasattr(EIU, 'upload_images')
assert hasattr(EIU, 'extract_from_zip')
assert hasattr(EIU, 'extract_from_pdf')
assert "x-pilot" in EIU.UPLOAD_URL
print(f"OK (URL: {EIU.UPLOAD_URL})")

print("\n" + "=" * 60)
print("ALL 7 TESTS PASSED")
print("=" * 60)
