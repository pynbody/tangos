"""Screenshots and videos of the web interface for tutorials/webserver, taken during the docs build.

docs/conf.py calls :func:`start_server` and :func:`capture` when the build starts,
and :func:`stop_server` when it finishes. The server runs ``tangos serve`` against
the tutorial database on its usual port, so ``tutorials/webserver`` can also fetch
data from it in its ``.. ipython::`` blocks. Each image is cropped to the part of
the page it is about; the one animation is assembled frame by frame with Pillow,
with each click marked by a circle where it lands. The videos of adding columns
are recorded with playwright's screencast, with the window sized to the table, a
pointer drawn in, and each click and key press marked; they go in ``videos/``,
which conf.py publishes as static files.

To run it on its own, with the tutorial database in place::

    python docs/web_screenshots.py docs/_web_screenshots

It needs playwright and a browser for it: ``pip install -e .[docs]`` and then
``python -m playwright install chromium``.
"""

import contextlib
import io
import os
import subprocess
import sys
import tempfile
import time
import urllib.request

PORT = 6543
BASE_URL = f"http://localhost:{PORT}"

VIEWPORT = {"width": 1200, "height": 900}
SCALE = 2      # device pixels per CSS pixel, so that the images stay sharp
PADDING = 8    # CSS pixels around each cropped element

_server = None
_server_log = None


def start_server(timeout=120):
    """Start ``tangos serve`` on PORT and wait until it answers."""
    global _server, _server_log
    if _server is not None:
        return
    if _answers():
        raise RuntimeError(f"Something is already listening on port {PORT}; stop it before "
                           f"building the documentation, which runs its own tangos server there.")
    # the server's output is kept, to be reported if it fails to start
    _server_log = tempfile.TemporaryFile(mode="w+")
    _server = subprocess.Popen(["tangos", "serve", "--title", "the tutorial database"],
                               stdout=_server_log, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + timeout
    while not _answers():
        if _server.poll() is not None:
            raise RuntimeError("tangos serve exited before it started answering:\n" + _read_log())
        if time.monotonic() > deadline:
            log = _read_log()
            stop_server()
            raise RuntimeError(f"tangos serve did not answer within {timeout} s:\n" + log)
        time.sleep(0.5)


def stop_server():
    global _server, _server_log
    if _server is not None:
        _server.terminate()
        _server.wait(timeout=30)
        _server = None
        _server_log.close()
        _server_log = None


def _read_log():
    _server_log.seek(0)
    return _server_log.read()


def _answers():
    try:
        with urllib.request.urlopen(BASE_URL + "/", timeout=5):
            return True
    except OSError:
        return False


class _Page:
    """A browser page with helpers for driving the tangos web interface."""

    def __init__(self, browser, output_dir, viewport=VIEWPORT):
        # a fresh context each time, since the interface remembers added columns
        # in session storage
        self._context = browser.new_context(viewport=viewport, device_scale_factor=SCALE)
        self.page = self._context.new_page()
        self.output_dir = output_dir

    def close(self):
        self._context.close()

    def goto(self, path):
        self.page.goto(BASE_URL + path)
        self.settle()

    def settle(self, extra_ms=1000):
        self.page.wait_for_load_state("networkidle")
        self.page.wait_for_timeout(extra_ms)

    def add_column(self, object_tag, expression):
        """Type an expression into the 'Add +' heading of a timestep's table."""
        heading = self.page.locator(f"#label-row-{object_tag} th.editable").last
        heading.click()
        self.page.keyboard.type(expression)
        # the autocompletion menu would otherwise take the Enter key
        self.page.evaluate("$('.ui-autocomplete-input').autocomplete('close')")
        self.page.keyboard.press("Enter")
        self.settle(1500)

    def property_row(self, name):
        return self.page.locator(
            "tr.tangos-fixed-row",
            has=self.page.locator(f"td[id^='label-fixed-row']:text-is('{name}')"))

    def plot(self, x, y, kind, logx=False, logy=False, wait_ms=4000):
        """Choose x and y on a halo page and plot them for this halo or this timestep."""
        self.property_row(x).locator("input[name=x]").check()
        self.property_row(y).locator("input[name=y]").check()
        self.page.locator("#logx").set_checked(logx)
        self.page.locator("#logy").set_checked(logy)
        self.page.locator(f"#type-{kind}").check()
        self.settle(wait_ms)

    def clip(self, *locators, max_height=None, max_width=None, pad_bottom=True):
        """The rectangle around the given elements, padded, and optionally cut short."""
        boxes = [locator.bounding_box() for locator in locators]
        left = min(b["x"] for b in boxes) - PADDING
        top = min(b["y"] for b in boxes) - PADDING
        right = max(b["x"] + b["width"] for b in boxes) + PADDING
        bottom = max(b["y"] + b["height"] for b in boxes) + (PADDING if pad_bottom else 0)
        if max_height is not None:
            bottom = min(bottom, top + max_height)
        if max_width is not None:
            right = min(right, left + max_width)
        return {"x": max(left, 0), "y": max(top, 0), "width": right - max(left, 0),
                "height": bottom - max(top, 0)}

    def shot(self, name, *locators, max_height=None, max_width=None):
        return self.shot_clip(name, self.clip(*locators, max_height=max_height,
                                              max_width=max_width))

    def shot_clip(self, name, clip):
        path = os.path.join(self.output_dir, name)
        self.page.screenshot(path=path, clip=clip, full_page=True)
        return path


class _Movie:
    """An animated PNG of a cropped region, built frame by frame.

    Each click is shown, before it happens, by a frame with a circle at the point
    clicked, so that a reader can follow what was done between frames."""

    FRAME_MS = 1800
    CLICK_MS = 700
    CLICK_RADIUS = 14             # CSS pixels
    CLICK_COLOUR = (0, 120, 255)  # a blue that stands out from the interface's orange

    def __init__(self, page, clip):
        self._page = page
        self._clip = clip
        self._frames = []
        self._durations = []

    def _grab(self):
        from PIL import Image
        png = self._page.page.screenshot(clip=self._clip, full_page=True)
        return Image.open(io.BytesIO(png)).convert("RGBA")

    def frame(self, duration_ms=FRAME_MS):
        self._frames.append(self._grab())
        self._durations.append(duration_ms)

    def click(self, locator, settle_ms=1000):
        """Add a frame marking where locator is about to be clicked, then click it."""
        from PIL import Image, ImageDraw
        box = locator.bounding_box()
        page_x, page_y = self._page.page.evaluate("() => [window.scrollX, window.scrollY]")
        x = (box["x"] + page_x + box["width"] / 2 - self._clip["x"]) * SCALE
        y = (box["y"] + page_y + box["height"] / 2 - self._clip["y"]) * SCALE
        r = self.CLICK_RADIUS * SCALE
        image = self._grab()
        overlay = Image.new("RGBA", image.size, (0, 0, 0, 0))
        draw = ImageDraw.Draw(overlay)
        draw.ellipse((x - r, y - r, x + r, y + r), fill=self.CLICK_COLOUR + (70,),
                     outline=self.CLICK_COLOUR + (255,), width=2 * SCALE)
        self._frames.append(Image.alpha_composite(image, overlay))
        self._durations.append(self.CLICK_MS)
        locator.click()
        self._page.settle(settle_ms)

    def save(self, name):
        self._frames[0].save(os.path.join(self._page.output_dir, name), save_all=True,
                             append_images=self._frames[1:], duration=self._durations, loop=0)


class _Video:
    """A video of the interface being used, recorded with playwright's screencast.

    The window is sized to frame just the part being used, so the video needs no
    cropping. Chromium must be launched with --force-device-scale-factor (see
    :func:`capture`): the screencast otherwise records one video pixel per CSS
    pixel, which looks soft. Playwright draws no mouse pointer, so the page is
    given one, with a ripple wherever a click lands, and a badge naming each key
    pressed that is not typed into the page."""

    VIEWPORT = {"width": 700, "height": 400}
    VIDEO_SIZE = {"width": VIEWPORT["width"] * SCALE, "height": VIEWPORT["height"] * SCALE}
    TYPING_DELAY_MS = 120

    _DECORATIONS = """() => {
        const style = document.createElement('style');
        style.textContent = `
          #docs-pointer { position: fixed; z-index: 100000; pointer-events: none;
                          width: 18px; height: 18px; left: -100px; top: -100px; }
          .docs-ripple { position: fixed; z-index: 99999; pointer-events: none;
                         width: 28px; height: 28px; margin: -14px 0 0 -14px;
                         border-radius: 50%; border: 2px solid rgb(0, 120, 255);
                         background: rgba(0, 120, 255, 0.25);
                         animation: docs-ripple 900ms ease-out forwards; }
          @keyframes docs-ripple { from { transform: scale(0.4); opacity: 1; }
                                   to { transform: scale(1.3); opacity: 0; } }
          #docs-key { position: fixed; z-index: 100000; pointer-events: none;
                      right: 12px; bottom: 12px; padding: 4px 10px; border-radius: 5px;
                      font: 600 15px sans-serif; color: white;
                      background: rgba(0, 120, 255, 0.9); display: none; }`;
        document.head.appendChild(style);
        const pointer = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
        pointer.id = 'docs-pointer';
        pointer.setAttribute('viewBox', '0 0 24 24');
        pointer.innerHTML = '<path d="M3 2 L3 20 L8 15 L11.5 22 L14.5 20.5 L11 13.8 L18 13.8 Z" '
                          + 'fill="black" stroke="white" stroke-width="1.5" stroke-linejoin="round"/>';
        document.body.appendChild(pointer);
        const key = document.createElement('div');
        key.id = 'docs-key';
        document.body.appendChild(key);
        document.addEventListener('mousemove', e => {
            pointer.style.left = e.clientX + 'px';
            pointer.style.top = e.clientY + 'px';
        }, true);
        document.addEventListener('mousedown', e => {
            const ripple = document.createElement('div');
            ripple.className = 'docs-ripple';
            ripple.style.left = e.clientX + 'px';
            ripple.style.top = e.clientY + 'px';
            document.body.appendChild(ripple);
            setTimeout(() => ripple.remove(), 1000);
        }, true);
        window.docsShowKey = (name) => {
            key.textContent = name;
            key.style.display = 'block';
            setTimeout(() => { key.style.display = 'none'; }, 900);
        };
    }"""

    def __init__(self, browser, output_dir, path, top):
        """Open path, scrolled so that the element matched by the selector top is
        at the top of the window."""
        self._page = _Page(browser, output_dir, viewport=self.VIEWPORT)
        self.page = self._page.page
        self._page.goto(path)
        self.page.evaluate(self._DECORATIONS)
        self.page.locator(top).first.evaluate(
            "e => { e.scrollIntoView(); window.scrollBy(0, -16); }")
        # start the pointer off to one side of the window
        self.page.mouse.move(self.VIEWPORT["width"] * 0.75, self.VIEWPORT["height"] * 0.6)
        self._page.settle(500)

    def close(self):
        self._page.close()

    @contextlib.contextmanager
    def record(self, name):
        video_dir = os.path.join(self._page.output_dir, "videos")
        os.makedirs(video_dir, exist_ok=True)
        with self.page.screencast.start(path=os.path.join(video_dir, name), size=self.VIDEO_SIZE):
            self.page.wait_for_timeout(1000)
            yield
            self.page.wait_for_timeout(2500)

    def click(self, locator, settle_ms=1500):
        """Move the pointer smoothly to the middle of locator, and click there."""
        box = locator.bounding_box()
        self.page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2, steps=25)
        self.page.wait_for_timeout(300)
        locator.click()
        self._page.settle(settle_ms)

    def press(self, locator, key, settle_ms=1500):
        self.page.evaluate("key => window.docsShowKey(key)", key)
        self.page.wait_for_timeout(300)
        locator.press(key)
        self._page.settle(settle_ms)

    def add_column(self, object_tag, expression):
        """Click the 'Add +' heading of a timestep's table, type, and press Enter."""
        heading = self.page.locator(f"#label-row-{object_tag} th.editable").last
        self.click(heading, settle_ms=400)
        # move the pointer aside, above and to the right, so as to hide neither
        # what is typed nor the autocompletion menu below it (hovering over the
        # menu would also highlight a suggestion, which Enter would accept)
        box = heading.bounding_box()
        self.page.mouse.move(box["x"] + box["width"] + 20, box["y"] - 20, steps=8)
        heading.press_sequentially(expression, delay=self.TYPING_DELAY_MS)
        self.page.wait_for_timeout(800)
        self.press(heading, "Enter")
        # pressing Enter accepts a highlighted autocompletion, which could change
        # what was typed (see tutorials/webserver)
        headings = self.page.locator(f"#label-row-{object_tag} th.editable").all_inner_texts()
        if expression not in headings:
            raise RuntimeError(f"Typing {expression!r} into a column heading gave {headings}")


def _browse(browser, output_dir):
    p = _Page(browser, output_dir)
    p.goto("/tutorial_gadget")
    p.shot("web_simulation.png", p.page.locator("table.sortable"), max_height=330)

    p.goto("/tutorial_gadget/snapshot_020")
    p.shot("web_timestep.png", p.page.locator("h1"), p.page.locator("#objects-container"),
           max_height=420, max_width=760)
    p.close()


def _halo(browser, output_dir):
    p = _Page(browser, output_dir)
    p.goto("/tutorial_gadget/snapshot_020/halo_1")
    title, navigation = p.page.locator("#halo-title"), p.page.locator("#navigation")
    properties = p.page.locator("#properties-area")
    # the plots panel sits to the right of the navigation box; stop short of it
    left_column = p.page.locator("#imgbox_container").bounding_box()["x"] - PADDING
    width = left_column - title.bounding_box()["x"]
    p.shot("web_halo.png", title, navigation, properties, max_height=600, max_width=width)

    # stepping back through time, one snapshot per frame
    movie = _Movie(p, p.clip(title, navigation, properties, max_height=460, max_width=width))
    movie.frame()
    for step in range(3):
        movie.click(p.page.locator("#navigation a:text-is('-1')"), settle_ms=2500)
        movie.frame()
    movie.save("web_time_navigation.png")
    p.close()

    # plots are drawn to fit the window, so a shorter one gives them a
    # proportion that suits the page
    p = _Page(browser, output_dir, viewport={"width": 1200, "height": 640})
    p.goto("/tutorial_gadget/snapshot_020/halo_1")
    form, plot = p.page.locator("#image_form"), p.page.locator("#imgbox img")
    p.plot("mass", "VMax", "gather", logx=True, logy=True)
    p.shot("web_population_plot.png", form, plot)
    p.shot_clip("web_plot_controls.png",
                p.clip(p.page.locator("#properties-area h2"), p.property_row("t()"),
                       p.property_row("VMax"), pad_bottom=False))
    p.plot("t()", "mass", "cascade", logy=True)
    p.shot("web_mass_history.png", form, plot)
    p.close()

    # the tree is drawn in a box sized to the window, which scrolls if the tree
    # is taller; this window is tall enough for the whole tree
    p = _Page(browser, output_dir, viewport={"width": 1200, "height": 1100})
    p.goto("/tutorial_gadget/snapshot_020/halo_1")
    p.page.locator("#type-tree").check()
    p.settle(6000)
    # the tree's drawing area is larger than the tree, so crop to what is drawn
    tree = p.page.evaluate("""() => {
        const r = document.querySelector('#imgbox svg g').getBoundingClientRect();
        return {x: r.x + window.scrollX, y: r.y + window.scrollY, width: r.width, height: r.height};
    }""")
    form = p.page.locator("#image_form").bounding_box()
    left, top = min(form["x"], tree["x"]) - PADDING, form["y"] - PADDING
    right = max(form["x"] + form["width"], tree["x"] + tree["width"]) + PADDING
    bottom = tree["y"] + tree["height"] + PADDING
    p.shot_clip("web_merger_tree.png", {"x": left, "y": top, "width": right - left,
                                        "height": bottom - top})
    p.close()


def _calculations(browser, output_dir):
    v = _Video(browser, output_dir, "/tutorial_gadget/snapshot_020", "#objects-container h2")
    with v.record("web_columns.webm"):
        v.add_column("halo", "mass")
        v.add_column("halo", "VMax")
    with v.record("web_filter.webm"):
        v.add_column("halo", "VMax<300")
        v.click(v.page.locator("#properties_form_halo .leftbuttons label.filter-checkbox").first)
    v.close()

    v = _Video(browser, output_dir, "/tutorial_changa_blackholes/%25960", "#objects-container h2")
    with v.record("web_link_column.webm"):
        v.add_column("halo", "BH_central.BH_mass")
    v.close()


def capture(output_dir):
    """Take every screenshot and video; the server must already be running."""
    from playwright.sync_api import sync_playwright

    os.makedirs(output_dir, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            _browse(browser, output_dir)
            _halo(browser, output_dir)
        finally:
            browser.close()
        # the screencast records at the browser's own scale factor, not the page's
        browser = playwright.chromium.launch(args=[f"--force-device-scale-factor={SCALE}"])
        try:
            _calculations(browser, output_dir)
        finally:
            browser.close()


if __name__ == "__main__":
    start_server()
    try:
        capture(sys.argv[1])
    finally:
        stop_server()
