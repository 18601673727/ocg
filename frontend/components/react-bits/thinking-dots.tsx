"use client";

import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type ReactNode,
} from "react";
import { Canvas, useFrame, useThree } from "@react-three/fiber";
import * as THREE from "three";
import { cn } from "@/lib/utils";

export interface ThinkingDotsProps {
  /** How fast the cloud drifts */
  speed?: number;
  /** Number of gaussian lobes making up the cloud */
  lobes?: number;
  /** Distance between dot centres, in screen heights */
  spacing?: number;
  /** Radius of a dot outside the cloud, as a fraction of a cell */
  dotSize?: number;
  /** Extra radius a dot gains at full cloud density */
  dotGain?: number;
  /** Edge softness of a dot, 0 is hard and 1 is fully feathered */
  dotSoftness?: number;
  /** Random per-cell variation in dot radius */
  jitter?: number;
  /** Depth of the size ripple travelling across the grid */
  pulse?: number;
  /** Speed of that ripple */
  pulseRate?: number;
  /** Depth of the matching brightness ripple */
  pulseGlow?: number;
  /** How far the lobes wander from the centre */
  drift?: number;
  /** Overall size of the cloud */
  cloudScale?: number;
  /** Strength of the noise that breaks up the cloud edge */
  turbulence?: number;
  /** Spatial frequency of that noise */
  turbulenceScale?: number;
  /** Brightness of dots outside the cloud */
  ambient?: number;
  /** Brightness a dot gains at full cloud density */
  intensity?: number;
  /** Density at which the accent colour starts appearing */
  accentStart?: number;
  /** Density at which the accent colour takes over */
  accentEnd?: number;
  /** Colour of the resting dots */
  color?: string;
  /** Colour of the dots at the heart of the cloud */
  accentColor?: string;
  /** Strength of the halo around the densest dots */
  glow?: number;
  /** How quickly that halo falls off inside a cell */
  glowFalloff?: number;
  /** Film grain strength, 0 to 1 */
  grain?: number;
  /** Grain refreshes per second */
  grainRate?: number;
  /** Corner darkening, 0 to 1 */
  vignette?: number;
  /** Panel backdrop, or "transparent" to show the page through */
  backgroundColor?: string;
  /** Master alpha */
  opacity?: number;
  /** Let the pointer draw the cloud toward it */
  cursorInteraction?: boolean;
  /** How strongly the pointer swells the cloud */
  cursorInfluence?: number;
  /** Freeze the animation */
  paused?: boolean;
  /** Scale back resolution when the frame budget slips */
  adaptiveQuality?: boolean;
  /** Frame rate the quality meter aims to hold */
  targetFps?: number;
  /** Upper device pixel ratio bound */
  dpr?: number;
  className?: string;
  children?: ReactNode;
}

const dotsVertex = `
varying vec2 vPlane;

void main() {
  vPlane = uv;
  gl_Position = vec4(position.xy * 2.0, 0.0, 1.0);
}
`;

const buildDotsFragment = (lobes: number) => `
precision highp float;

varying vec2 vPlane;

uniform vec2 uCanvas;
uniform float uClock;
uniform float uSpeed;
uniform float uSpacing;
uniform float uDotSize;
uniform float uDotGain;
uniform float uDotSoft;
uniform float uJitter;
uniform float uPulse;
uniform float uPulseRate;
uniform float uPulseGlow;
uniform float uDrift;
uniform float uCloudScale;
uniform float uTurbulence;
uniform float uTurbScale;
uniform float uAmbient;
uniform float uIntensity;
uniform float uAccentLow;
uniform float uAccentHigh;
uniform float uGlow;
uniform float uGlowFalloff;
uniform float uGrain;
uniform float uGrainRate;
uniform float uVignette;
uniform vec3 uInk;
uniform vec3 uAccent;
uniform vec3 uBackdrop;
uniform float uBackdropAlpha;
uniform float uOpacity;
uniform vec2 uPointer;
uniform float uPointerPull;

float spark(vec2 seed) {
  vec3 drift = fract(vec3(seed.xyx) * vec3(0.1031, 0.1030, 0.0973));
  drift += dot(drift, drift.yzx + 33.33);
  return fract((drift.x + drift.y) * drift.z);
}

float lattice(vec2 p) {
  vec2 base = floor(p);
  vec2 slide = fract(p);
  slide = slide * slide * (3.0 - 2.0 * slide);
  float a = spark(base);
  float b = spark(base + vec2(1.0, 0.0));
  float c = spark(base + vec2(0.0, 1.0));
  float d = spark(base + vec2(1.0, 1.0));
  return mix(mix(a, b, slide.x), mix(c, d, slide.x), slide.y);
}

float churn(vec2 p) {
  mat2 twist = mat2(0.8, 0.6, -0.6, 0.8);
  float sum = 0.0;
  float weight = 0.5;
  for (int k = 0; k < 3; k++) {
    sum += weight * lattice(p);
    p = twist * p * 2.07 + 9.3;
    weight *= 0.5;
  }
  return sum;
}

float lobe(vec2 p, vec2 seat, vec2 stretch, float tilt) {
  float c = cos(tilt);
  float s = sin(tilt);
  vec2 q = mat2(c, -s, s, c) * (p - seat);
  q /= max(stretch, vec2(0.02));
  return exp(-dot(q, q) * 2.4);
}

float cloud(vec2 p, float t) {
  float sum = 0.0;

  for (int i = 0; i < ${lobes}; i++) {
    float k = float(i);
    vec2 seat = uDrift * vec2(
      sin(t * (0.55 - k * 0.09) + k * 1.9),
      cos(t * (0.37 + k * 0.05) + k * 1.1)
    );
    vec2 stretch = uCloudScale * vec2(0.46 - k * 0.06, 0.30 + k * 0.04);
    float tilt = 0.35 * sin(t * 0.32 + k * 2.1) + k * 0.7;
    sum += lobe(p, seat, stretch, tilt) * pow(0.78, k);
  }

  sum += (churn(p * uTurbScale + vec2(t * 0.18, -t * 0.11)) - 0.5) * uTurbulence;
  sum += lobe(p, uPointer, vec2(uCloudScale * 0.52, uCloudScale * 0.40), 0.0) *
    uPointerPull;

  return max(sum, 0.0);
}

void main() {
  vec2 pixel = vPlane * uCanvas;
  vec2 field = (pixel - 0.5 * uCanvas) / max(uCanvas.y, 1.0);

  float t = uClock * uSpeed;
  float pitch = max(uSpacing, 0.004);

  vec2 grid = field / pitch;
  vec2 cell = floor(grid);
  vec2 local = fract(grid) - 0.5;
  vec2 seat = (cell + 0.5) * pitch;

  float density = cloud(seat, t);
  float noise = spark(cell + 0.5);
  float beat = 0.5 + 0.5 * sin(t * uPulseRate + cell.x * 0.7 + cell.y * 0.43);

  float radius = uDotSize + density * uDotGain + noise * uJitter;
  radius *= (1.0 - uPulse) + uPulse * beat;
  radius = max(radius, 1e-4);

  float inner = radius * clamp(1.0 - uDotSoft * 0.9, 0.02, 0.99);
  float ink = smoothstep(radius, inner, length(local));

  float accent = smoothstep(uAccentLow, uAccentHigh, density);
  accent *= (1.0 - uPulseGlow * 0.6) + uPulseGlow * 0.6 * beat;
  vec3 tint = mix(uInk, uAccent, clamp(accent, 0.0, 1.0));

  float level = uAmbient + density * uIntensity;
  level *= (1.0 - uPulseGlow) + uPulseGlow * beat;

  float mass = clamp(ink * max(level, 0.0), 0.0, 1.0);
  vec3 col = tint * mass;
  float cover = mass;

  float halo = smoothstep(uAccentLow * 0.9, uAccentHigh * 1.05, density);
  halo *= exp(-length(local) * uGlowFalloff) * uGlow;
  col += uAccent * halo;
  cover = clamp(cover + halo, 0.0, 1.0);

  float vig = smoothstep(1.1, 0.2, length(field));
  float shade = (1.0 - uVignette) + uVignette * vig;
  col *= shade;
  cover *= shade;

  float tick = floor(uClock * max(uGrainRate, 1.0));
  float speck = spark(pixel + tick * 17.0) - 0.5;
  float dust = 1.0 + speck * uGrain * cover * (1.0 - cover) * 8.0;
  col = clamp(col * dust, 0.0, 1.0);
  cover = clamp(cover * dust, 0.0, 1.0);

  float rest = uBackdropAlpha * (1.0 - cover);
  gl_FragColor = vec4(col + uBackdrop * rest, cover + rest) * uOpacity;
}
`;

const literal = (hex: string, fallback: string) => {
  const shade = new THREE.Color();
  try {
    shade.setStyle(hex, THREE.LinearSRGBColorSpace);
  } catch {
    shade.setStyle(fallback, THREE.LinearSRGBColorSpace);
  }
  return shade;
};

const repaint = (target: THREE.Color, hex: string) => {
  try {
    target.setStyle(hex, THREE.LinearSRGBColorSpace);
  } catch {
    return;
  }
};

const clamp = (value: number, low: number, high: number) =>
  Math.min(high, Math.max(low, value));

const subscribeToScreen = (notify: () => void) => {
  if (typeof window === "undefined") return () => {};
  const media = window.matchMedia("(min-resolution: 2dppx)");
  media.addEventListener("change", notify);
  window.addEventListener("resize", notify);
  return () => {
    media.removeEventListener("change", notify);
    window.removeEventListener("resize", notify);
  };
};

const readScreenDpr = () =>
  typeof window === "undefined" ? 1 : window.devicePixelRatio || 1;

const isClear = (paint: string) =>
  paint === "transparent" || paint === "none" || paint === "";

interface PointerState {
  x: number;
  y: number;
  inside: boolean;
}

interface DotFieldProps {
  speed: number;
  lobes: number;
  spacing: number;
  dotSize: number;
  dotGain: number;
  dotSoftness: number;
  jitter: number;
  pulse: number;
  pulseRate: number;
  pulseGlow: number;
  drift: number;
  cloudScale: number;
  turbulence: number;
  turbulenceScale: number;
  ambient: number;
  intensity: number;
  accentStart: number;
  accentEnd: number;
  color: string;
  accentColor: string;
  glow: number;
  glowFalloff: number;
  grain: number;
  grainRate: number;
  vignette: number;
  backgroundColor: string;
  opacity: number;
  cursorInteraction: boolean;
  cursorInfluence: number;
  paused: boolean;
  adaptiveQuality: boolean;
  targetFps: number;
  ceiling: number;
  readPointer: () => PointerState;
}

const DotField = ({
  speed,
  lobes,
  spacing,
  dotSize,
  dotGain,
  dotSoftness,
  jitter,
  pulse,
  pulseRate,
  pulseGlow,
  drift,
  cloudScale,
  turbulence,
  turbulenceScale,
  ambient,
  intensity,
  accentStart,
  accentEnd,
  color,
  accentColor,
  glow,
  glowFalloff,
  grain,
  grainRate,
  vignette,
  backgroundColor,
  opacity,
  cursorInteraction,
  cursorInfluence,
  paused,
  adaptiveQuality,
  targetFps,
  ceiling,
  readPointer,
}: DotFieldProps) => {
  const materialRef = useRef<THREE.ShaderMaterial>(null);
  const clock = useRef(0);
  const glide = useRef({ x: 0, y: 0, pull: 0 });
  const budget = useRef({ frames: 0, span: 0, wins: 0, cap: 4 });
  const { gl, size } = useThree();

  const puffs = Math.round(clamp(lobes, 1, 8));

  const fragment = useMemo(() => buildDotsFragment(puffs), [puffs]);

  const uniforms = useMemo(
    () => ({
      uCanvas: { value: new THREE.Vector2(1, 1) },
      uClock: { value: 0 },
      uSpeed: { value: 1 },
      uSpacing: { value: 0.058 },
      uDotSize: { value: 0.022 },
      uDotGain: { value: 0.112 },
      uDotSoft: { value: 0.48 },
      uJitter: { value: 0.01 },
      uPulse: { value: 0.1 },
      uPulseRate: { value: 1.6 },
      uPulseGlow: { value: 0.22 },
      uDrift: { value: 0.22 },
      uCloudScale: { value: 1 },
      uTurbulence: { value: 0.22 },
      uTurbScale: { value: 2.4 },
      uAmbient: { value: 0.16 },
      uIntensity: { value: 0.86 },
      uAccentLow: { value: 0.55 },
      uAccentHigh: { value: 1.1 },
      uGlow: { value: 0.035 },
      uGlowFalloff: { value: 4 },
      uGrain: { value: 0.03 },
      uGrainRate: { value: 24 },
      uVignette: { value: 0.1 },
      uInk: { value: literal("#ff44af", "#ff44af") },
      uAccent: { value: literal("#ff44af", "#ff44af") },
      uBackdrop: { value: literal("#0a0a0a", "#0a0a0a") },
      uBackdropAlpha: { value: 1 },
      uOpacity: { value: 1 },
      uPointer: { value: new THREE.Vector2(0, 0) },
      uPointerPull: { value: 0 },
    }),
    [],
  );

  useEffect(() => {
    const material = materialRef.current;
    if (!material) return;
    material.fragmentShader = fragment;
    material.needsUpdate = true;
  }, [fragment]);

  useEffect(() => {
    const material = materialRef.current;
    if (!material) return;
    repaint(material.uniforms.uInk.value, color);
    repaint(material.uniforms.uAccent.value, accentColor);
    const clear = isClear(backgroundColor);
    material.uniforms.uBackdropAlpha.value = clear ? 0 : 1;
    if (!clear) repaint(material.uniforms.uBackdrop.value, backgroundColor);
  }, [color, accentColor, backgroundColor]);

  useFrame((_, delta) => {
    const material = materialRef.current;
    if (!material) return;

    const beat = Math.min(delta, 0.05);
    if (!paused) clock.current += beat;

    const ratio = gl.getPixelRatio();
    const set = material.uniforms;
    set.uCanvas.value.set(size.width * ratio, size.height * ratio);
    set.uClock.value = clock.current;
    set.uSpeed.value = speed;
    set.uSpacing.value = spacing;
    set.uDotSize.value = dotSize;
    set.uDotGain.value = dotGain;
    set.uDotSoft.value = dotSoftness;
    set.uJitter.value = jitter;
    set.uPulse.value = pulse;
    set.uPulseRate.value = pulseRate;
    set.uPulseGlow.value = pulseGlow;
    set.uDrift.value = drift;
    set.uCloudScale.value = cloudScale;
    set.uTurbulence.value = turbulence;
    set.uTurbScale.value = turbulenceScale;
    set.uAmbient.value = ambient;
    set.uIntensity.value = intensity;
    set.uAccentLow.value = accentStart;
    set.uAccentHigh.value = Math.max(accentEnd, accentStart + 0.01);
    set.uGlow.value = glow;
    set.uGlowFalloff.value = glowFalloff;
    set.uGrain.value = grain;
    set.uGrainRate.value = grainRate;
    set.uVignette.value = vignette;
    set.uOpacity.value = opacity;

    const span = size.height || 1;
    const wide = (size.width || 1) / span;
    const pointer = readPointer();
    const ease = 1 - Math.exp(-beat * 5);
    const aimX = cursorInteraction ? (pointer.x - 0.5) * wide : 0;
    const aimY = cursorInteraction ? pointer.y - 0.5 : 0;
    const aimPull =
      cursorInteraction && pointer.inside ? Math.max(cursorInfluence, 0) : 0;
    glide.current.x += (aimX - glide.current.x) * ease;
    glide.current.y += (aimY - glide.current.y) * ease;
    glide.current.pull += (aimPull - glide.current.pull) * ease;
    set.uPointer.value.set(glide.current.x, glide.current.y);
    set.uPointerPull.value = glide.current.pull;

    if (!adaptiveQuality) return;
    const meter = budget.current;
    meter.frames += 1;
    meter.span += delta;
    if (meter.span < 0.75) return;
    const fps = meter.frames / meter.span;
    meter.frames = 0;
    meter.span = 0;
    const roof = Math.min(ceiling, meter.cap);
    if (fps < targetFps * 0.85 && ratio > 0.6) {
      meter.wins = 0;
      meter.cap = Math.max(0.6, ratio * 0.9);
      gl.setPixelRatio(Math.max(0.6, ratio * 0.75));
    } else if (fps > targetFps * 0.98 && ratio < roof - 0.01) {
      meter.wins += 1;
      if (meter.wins >= 3) {
        meter.wins = 0;
        gl.setPixelRatio(Math.min(roof, ratio * 1.25));
      }
    } else {
      meter.wins = 0;
    }
  });

  return (
    <mesh frustumCulled={false}>
      <planeGeometry args={[1, 1]} />
      <shaderMaterial
        ref={materialRef}
        vertexShader={dotsVertex}
        fragmentShader={fragment}
        uniforms={uniforms}
        transparent
        depthTest={false}
        depthWrite={false}
        premultipliedAlpha
      />
    </mesh>
  );
};

export const ThinkingDots = ({
  speed = 1,
  lobes = 3,
  spacing = 0.058,
  dotSize = 0.022,
  dotGain = 0.112,
  dotSoftness = 0.48,
  jitter = 0.01,
  pulse = 0.1,
  pulseRate = 1.6,
  pulseGlow = 0.22,
  drift = 0.22,
  cloudScale = 1,
  turbulence = 0.22,
  turbulenceScale = 2.4,
  ambient = 0.16,
  intensity = 0.86,
  accentStart = 0.55,
  accentEnd = 1.1,
  color = "#ff44af",
  accentColor = "#ff44af",
  glow = 0.035,
  glowFalloff = 4,
  grain = 0.03,
  grainRate = 24,
  vignette = 0.1,
  backgroundColor = "#0a0a0a",
  opacity = 1,
  cursorInteraction = true,
  cursorInfluence = 0.35,
  paused = false,
  adaptiveQuality = true,
  targetFps = 60,
  dpr = 1.75,
  className,
  children,
}: ThinkingDotsProps) => {
  const shell = useRef<HTMLDivElement>(null);
  const pointer = useRef<PointerState>({ x: 0.5, y: 0.5, inside: false });
  const [awake, setAwake] = useState(true);

  const screenDpr = useSyncExternalStore(
    subscribeToScreen,
    readScreenDpr,
    () => 1,
  );
  const ceiling = Math.min(screenDpr, Math.max(dpr, 0.5));

  const readPointer = useCallback(() => pointer.current, []);

  useEffect(() => {
    const node = shell.current;
    if (!node || typeof IntersectionObserver === "undefined") return;
    const watcher = new IntersectionObserver(
      ([entry]) => setAwake(entry.isIntersecting),
      { threshold: 0 },
    );
    watcher.observe(node);
    return () => watcher.disconnect();
  }, []);

  useEffect(() => {
    const node = shell.current;
    if (!node || !cursorInteraction) return;

    const track = (event: PointerEvent) => {
      const box = node.getBoundingClientRect();
      if (!box.width || !box.height) return;
      pointer.current.x = clamp((event.clientX - box.left) / box.width, 0, 1);
      pointer.current.y = clamp(
        1 - (event.clientY - box.top) / box.height,
        0,
        1,
      );
      pointer.current.inside = true;
    };

    const reset = () => {
      pointer.current.inside = false;
    };

    node.addEventListener("pointermove", track);
    node.addEventListener("pointerleave", reset);
    return () => {
      node.removeEventListener("pointermove", track);
      node.removeEventListener("pointerleave", reset);
    };
  }, [cursorInteraction]);

  return (
    <div ref={shell} className={cn("relative overflow-hidden", className)}>
      <div className="absolute inset-0">
        <Canvas
          orthographic
          dpr={ceiling}
          frameloop={awake ? "always" : "demand"}
          gl={{
            antialias: false,
            alpha: true,
            powerPreference: "high-performance",
          }}
        >
          <DotField
            speed={speed}
            lobes={lobes}
            spacing={spacing}
            dotSize={dotSize}
            dotGain={dotGain}
            dotSoftness={dotSoftness}
            jitter={jitter}
            pulse={pulse}
            pulseRate={pulseRate}
            pulseGlow={pulseGlow}
            drift={drift}
            cloudScale={cloudScale}
            turbulence={turbulence}
            turbulenceScale={turbulenceScale}
            ambient={ambient}
            intensity={intensity}
            accentStart={accentStart}
            accentEnd={accentEnd}
            color={color}
            accentColor={accentColor}
            glow={glow}
            glowFalloff={glowFalloff}
            grain={grain}
            grainRate={grainRate}
            vignette={vignette}
            backgroundColor={backgroundColor}
            opacity={opacity}
            cursorInteraction={cursorInteraction}
            cursorInfluence={cursorInfluence}
            paused={paused}
            adaptiveQuality={adaptiveQuality}
            targetFps={targetFps}
            ceiling={ceiling}
            readPointer={readPointer}
          />
        </Canvas>
      </div>
      {children ? (
        <div className="relative z-10 h-full w-full">{children}</div>
      ) : null}
    </div>
  );
};

export default ThinkingDots;
