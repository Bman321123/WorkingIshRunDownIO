import type { Config } from "tailwindcss";

export default {
  content: ["./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      boxShadow: {
        card: "0 10px 30px rgba(0,0,0,0.18)",
      },
    },
  },
  plugins: [],
} satisfies Config;

