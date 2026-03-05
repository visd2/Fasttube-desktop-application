"""
Run this once to generate placeholder icons for FastTube.
Requires: pip install pillow
"""
try:
    from PIL import Image, ImageDraw, ImageFont
    import os

    size = 256
    img = Image.new("RGBA", (size, size), (15, 15, 15, 255))
    draw = ImageDraw.Draw(img)

    # Red circle background
    draw.ellipse([20, 20, 236, 236], fill=(255, 0, 0, 255))

    # Lightning bolt ⚡ (simple polygon)
    bolt = [
        (128, 40), (90, 130), (118, 130),
        (100, 220), (160, 110), (130, 110), (155, 40)
    ]
    draw.polygon(bolt, fill=(255, 255, 255, 255))

    out_dir = os.path.join(os.path.dirname(__file__), "assets")
    os.makedirs(out_dir, exist_ok=True)

    png_path = os.path.join(out_dir, "icon.png")
    ico_path = os.path.join(out_dir, "icon.ico")
    logo_path = os.path.join(out_dir, "logo.png")

    img.save(png_path)
    img.save(ico_path, sizes=[(16,16),(32,32),(48,48),(64,64),(128,128),(256,256)])
    img.save(logo_path)

    print(f"Icons saved to: {out_dir}")

except ImportError:
    print("Pillow not installed. Run: pip install pillow")
    print("Icons are optional — the app runs without them.")
