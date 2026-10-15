import type { NextConfig } from "next";

// The production build is a static export: the OCG process serves it from
// the same origin as its authenticated API. No server features.
const nextConfig: NextConfig = {
  output: "export",
};

export default nextConfig;
