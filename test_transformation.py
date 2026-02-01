import json
import unittest
import sys
import os

# Add current directory to path so we can import the script
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from dify_transformation import main, extract_outline, extract_subtitles, extract_data_sources

class TestDifyTransformation(unittest.TestCase):
    
    def setUp(self):
        # Sample script data from user example
        self.script_json = """
        {
          "title": "行星运动的轨迹",
          "scenes": [
            {
              "type": "cover",
              "id": "scene_cover",
              "title": "行星运动的轨迹",
              "scene_knowledge": "Introduction to planetary motion.",
              "estimated_duration_seconds": 3.0
            },
            {
              "type": "normal",
              "id": "scene_01",
              "target": "Introduce concept",
              "subtitles": [
                {
                  "id": "sub_01",
                  "text": "Hello world",
                  "start_time_seconds": 3.0,
                  "end_time_seconds": 5.0
                }
              ],
              "estimated_duration_seconds": 15.0
            }
          ]
        }
        """
        
        # Sample resource data from user example
        self.resource_json = """
        [
            {
                "web_data": {
                    "comprehensive_data": {
                        "all_source_list": [
                            {
                                "type": "web",
                                "title": "ADAS Source",
                                "url": "https://example.com/adas",
                                "snippet": "ADAS details...",
                                "source": "ADAS Eye"
                            }
                        ]
                    }
                }
            }
        ]
        """

    def test_outline_extraction(self):
        script_data = json.loads(self.script_json)
        outline = extract_outline(script_data)
        
        self.assertEqual(len(outline), 2)
        self.assertEqual(outline[0]["title"], "行星运动的轨迹")
        self.assertEqual(outline[0]["startTime"], "00:00")
        self.assertEqual(outline[0]["endTime"], "00:03")
        
        self.assertEqual(outline[1]["startTime"], "00:03")
        self.assertEqual(outline[1]["endTime"], "00:18") # 3 + 15

    def test_subtitle_extraction(self):
        script_data = json.loads(self.script_json)
        subtitles = extract_subtitles(script_data)
        
        self.assertEqual(len(subtitles), 1)
        self.assertEqual(subtitles[0]["text"], "Hello world")
        self.assertEqual(subtitles[0]["startTime"], "00:03")

    def test_data_source_extraction(self):
        resource_data = json.loads(self.resource_json)
        sources = extract_data_sources(resource_data)
        
        self.assertEqual(len(sources), 1)
        self.assertEqual(sources[0]["type"], "WEB")
        self.assertEqual(sources[0]["title"], "ADAS Source")
        self.assertEqual(sources[0]["source_name"], "ADAS Eye")

    def test_main_function(self):
        result = main(self.script_json, self.resource_json)
        self.assertIn("result", result)
        self.assertIn("outline", result["result"])
        self.assertIn("subtitles", result["result"])
        self.assertIn("data_sources", result["result"])

    def test_robustness_string_input(self):
        # Test with string input that might be wrapped in markdown
        markdown_json = "```json\n" + self.script_json + "\n```"
        result = main(markdown_json, self.resource_json)
        self.assertIn("result", result)
        self.assertEqual(len(result["result"]["outline"]), 2)

if __name__ == '__main__':
    unittest.main()
