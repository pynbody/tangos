"""Screenshots of the web interface for tutorials/webserver, taken during the docs build.

docs/conf.py calls :func:`start_server` and :func:`capture` when the build starts,
and :func:`stop_server` when it finishes. The server runs ``tangos serve`` against
the tutorial database on its usual port, so ``tutorials/webserver`` can also fetch
data from it in its ``.. ipython::`` blocks. Each image is cropped to the part of
the page it is about; the one animation is assembled frame by frame with Pillow.

To run it on its own, with the tutorial database in place::

    python docs/web_screenshots.py docs/_web_screenshots

It needs playwright and a browser for it: ``pip install -e .[docs]`` and then
``python -m playwright install chromium``.
"""

import os
import subprocess
import sys
import time
import urllib.request

PORT = 6543
BASE_URL = f"http://localhost:{PORT}"

VIEWPORT = {"width": 1200, "height": 900}
SCALE = 2      # device pixels per CSS pixel, so that the images stay sharp
PADDING = 8    # CSS pixels around each cropped element

_server = None


def start_server(timeout=120):
    """Start ``tangos serve`` on PORT and wait until it answers."""
    global _server
    if _server is not None:
        return
    if _answers():
        raise RuntimeError(f"Something is already listening on port {PORT}; stop it before "
                           f"building the documentation, which runs its own tangos server there.")
    _server = subprocess.Popen(["tangos", "serve", "--title", "the tutorial database"],
                               stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + timeout
    while not _answers():
        if _server.poll() is not None:
            raise RuntimeError("tangos serve exited before it started answering")
        if time.time() > deadline:
            stop_server()
            raise RuntimeError(f"tangos serve did not answer within {timeout} s")
        time.sleep(0.5)


def stop_server():
    global _server
    if _server is not None:
        _server.terminate()
        _server.wait(timeout=30)
        _server = None


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


def _browse(browser, output_dir):
    p = _Page(browser, output_dir)
    p.goto("/tutorial_gadget")
    p.shot("web_simulation.png", p.page.locator("table.sortable"), max_height=330)

    p.goto("/tutorial_gadget/snapshot_020")
    p.shot("web_timestep.png", p.page.locator("h1"), p.page.locator("#objects-container"),
           max_height=420, max_width=760)
    p.close()


def _halo(browser, output_dir):
    from PIL import Image

    p = _Page(browser, output_dir)
    p.goto("/tutorial_gadget/snapshot_020/halo_1")
    title, navigation = p.page.locator("#halo-title"), p.page.locator("#navigation")
    properties = p.page.locator("#properties-area")
    # the plots panel sits to the right of the navigation box; stop short of it
    left_column = p.page.locator("#imgbox_container").bounding_box()["x"] - PADDING
    width = left_column - title.bounding_box()["x"]
    p.shot("web_halo.png", title, navigation, properties, max_height=600, max_width=width)

    # stepping back through time, one snapshot per frame
    clip = p.clip(title, navigation, properties, max_height=460, max_width=width)
    frames = []
    for step in range(4):
        if step > 0:
            p.page.locator("#navigation a:text-is('-1')").click()
            p.settle(2500)
        path = os.path.join(output_dir, f"_frame_{step}.png")
        p.page.screenshot(path=path, clip=clip, full_page=True)
        frames.append(Image.open(path))
    frames[0].save(os.path.join(output_dir, "web_time_navigation.png"), save_all=True,
                   append_images=frames[1:], duration=1800, loop=0)
    for step in range(4):
        os.remove(os.path.join(output_dir, f"_frame_{step}.png"))
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
    p = _Page(browser, output_dir)
    p.goto("/tutorial_gadget/snapshot_020")
    table = p.page.locator("#properties_form_halo")
    p.add_column("halo", "mass")
    p.add_column("halo", "VMax")
    p.shot("web_columns.png", table, max_height=330)

    p.add_column("halo", "VMax<300")
    p.shot("web_boolean_column.png", table, max_height=330)
    p.page.locator("#properties_form_halo .leftbuttons label.filter-checkbox").first.click()
    p.settle(1500)
    p.shot("web_filtered.png", table, max_height=330)
    p.close()

    p = _Page(browser, output_dir)
    p.goto("/tutorial_changa_blackholes/%25960")
    p.add_column("halo", "BH_central.BH_mass")
    p.shot("web_link_column.png", p.page.locator("#properties_form_halo"), max_height=330)
    p.close()


def capture(output_dir):
    """Take every screenshot; the server must already be running."""
    from playwright.sync_api import sync_playwright

    os.makedirs(output_dir, exist_ok=True)
    with sync_playwright() as playwright:
        browser = playwright.chromium.launch()
        try:
            _browse(browser, output_dir)
            _halo(browser, output_dir)
            _calculations(browser, output_dir)
        finally:
            browser.close()


if __name__ == "__main__":
    start_server()
    try:
        capture(sys.argv[1])
    finally:
        stop_server()
