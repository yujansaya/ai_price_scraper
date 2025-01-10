from selenium import webdriver


class WebDriverManager:
    def __init__(self, download_dir="/Users/aya/PycharmProjects/pythonProject/files"):
        self.download_dir = download_dir
        self.driver = None
        self._initialize_driver()

    def _initialize_driver(self):
        """Initialize the Chrome WebDriver with the specified options."""
        options = webdriver.ChromeOptions()

        # Set DNS over HTTPS configuration
        local_state = {
            "dns_over_https.mode": "secure",
            "dns_over_https.templates": "https://chrome.cloudflare-dns.com/dns-query",
        }
        options.add_experimental_option('localState', local_state)

        # Configure user agent
        user_agent = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.50 Safari/537.36'
        options.add_argument(f'user-agent={user_agent}')

        # Headless mode for silent execution
        options.add_argument('headless')

        # Other options
        options.add_argument("--remote-debugging-port=9222")
        options.add_argument("--disable-extensions")

        # Set download preferences
        options.add_experimental_option("prefs", {
            "download.default_directory": self.download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "plugins.always_open_pdf_externally": True,
            "savefile.default_directory": self.download_dir
        })

        # Initialize the driver
        self.driver = webdriver.Chrome(options=options)

    def get_driver(self):
        """Return the WebDriver instance."""
        if self.driver is None:
            raise RuntimeError("Driver is not initialized.")
        return self.driver

    def quit_driver(self):
        """Quit the WebDriver instance."""
        if self.driver:
            self.driver.quit()
            self.driver = None

    def __enter__(self):
        """Enable the class to be used as a context manager."""
        return self.get_driver()

    def __exit__(self, exc_type, exc_value, traceback):
        """Ensure the driver quits when the context ends."""
        self.quit_driver()
