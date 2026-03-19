import "./globals.css";

export const metadata = {
  title: "Workspace Hub Desktop",
  description: "Electron + Next.js operator workspace for the Codex Obsidian system.",
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
