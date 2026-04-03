import sys
try:
    from paddleocr import PaddleOCR
    import numpy as np
    from PIL import Image
    
    print("PaddleOCR imported successfully")
    # Initialize PaddleOCR (downloader will run on first call if not already there)
    ocr = PaddleOCR(use_angle_cls=True, lang='en', show_log=False)
    print("PaddleOCR initialized successfully")
    
    # Test with a dummy image (black square)
    dummy_img = np.zeros((100, 100, 3), dtype=np.uint8)
    result = ocr.ocr(dummy_img, cls=True)
    print("PaddleOCR test run successful")
    print(f"Result: {result}")
    
except ImportError as e:
    print(f"ImportError: {e}")
except Exception as e:
    print(f"Error during PaddleOCR initialization: {e}")
