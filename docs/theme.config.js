/**
 * @type {import("nextra-theme-docs").DocsThemeConfig}
 */
export default {
  project: { link: "https://github.com/bongani-m/hardhatdb" },
  docsRepositoryBase: "https://github.com/bongani-m/hardhatdb/tree/main/docs",
  useNextSeoProps() {
    return { titleTemplate: "%s – HardhatDB" };
  },
  primaryHue: { dark: 38, light: 36 },
  logo: (
    <>
      <span className="font-bold" style={{ marginRight: 8 }}>
        HardhatDB
      </span>
      <span className="text-gray-600 font-normal hidden md:inline">
        A MySQL server backed by Badger, replicated with Raft
      </span>
    </>
  ),
  head: (
    <>
      <meta name="msapplication-TileColor" content="#ffffff" />
      <meta name="theme-color" content="#ffffff" />
      <meta name="viewport" content="width=device-width, initial-scale=1.0" />
      <meta httpEquiv="Content-Language" content="en" />
      <meta
        name="description"
        content="HardhatDB: a MySQL server backed by one Badger directory, with Raft replication"
      />
      <meta
        name="og:description"
        content="HardhatDB: a MySQL server backed by one Badger directory, with Raft replication"
      />
      <meta name="twitter:card" content="summary_large_image" />
      <meta name="twitter:site:domain" content="https://github.com/bongani-m/hardhatdb" />
      <meta name="twitter:url" content="https://github.com/bongani-m/hardhatdb" />
      <meta
        name="og:title"
        content="HardhatDB: a MySQL server backed by Badger, replicated with Raft"
      />
      <meta name="apple-mobile-web-app-title" content="HardhatDB" />
    </>
  ),
  navigation: true,
  footer: { text: <>Apache-2.0 {new Date().getFullYear()} © HardhatDB.</> },
  editLink: { text: "Edit this page on GitHub" },
  unstable_faviconGlyph: "⛑",
};
