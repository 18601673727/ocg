import { type ComponentProps, insert, type JSX, type ValidComponent } from "@solidjs/web";
import { createSignal, omit, onCleanup } from "solid-js";

export type View = JSX.Element;
export type CSSProperties = JSX.CSSProperties;
export type ElementProps<T extends ValidComponent> = ComponentProps<T>;

export type Ref<T> = T | ((element: T) => void) | ElementRef<T | null>;
type ElementRef<T> = { current: T };
export type FormEvent<T = HTMLFormElement> = Event & { currentTarget: T };

export function elementRef<T>(value: T): ElementRef<T>;
export function elementRef<T>(value: null): ElementRef<T | null>;
export function elementRef<T>(value?: T) {
  return { current: value };
}

export function bindElementRef<T>(ref: ElementRef<T | null>) {
  return (node: T) => {
    ref.current = node;
    onCleanup(() => {
      if (ref.current === node) ref.current = null;
    });
  };
}

export function assignRef<T>(ref: Ref<T> | undefined, node: T) {
  if (typeof ref === "function") (ref as (value: T) => void)(node);
  else if (ref && typeof ref === "object" && "current" in ref) ref.current = node;
}

export function Group(props: { children?: View }) {
  return props.children;
}

export function createSubscription<T>(
  subscribe: (changed: () => void) => () => void,
  read: () => T,
) {
  const [value, setValue] = createSignal<T>(() => read(), { ownedWrite: true });
  onCleanup(subscribe(() => setValue(() => read())));
  return value;
}

function shallowEqual<T>(a: T, b: T) {
  if (Object.is(a, b)) return true;
  if (a instanceof Set && b instanceof Set)
    return a.size === b.size && [...a].every((item) => b.has(item));
  if (!a || !b || typeof a !== "object" || typeof b !== "object") return false;
  const keys = Object.keys(a);
  return (
    keys.length === Object.keys(b).length &&
    keys.every(
      (key) => Object.hasOwn(b, key) && Object.is(Reflect.get(a, key), Reflect.get(b, key)),
    )
  );
}

export function shallowSelector<T, U>(select: (state: T) => U): (state: T) => U {
  let previous: U;
  let initial = true;
  return (value) => {
    const next = select(value);
    if (initial || !shallowEqual(previous, next)) {
      previous = next;
      initial = false;
    }
    return previous;
  };
}

export function handleEvent<T extends Event>(
  listener: ((event: T) => void) | { 0: (data: never, event: T) => void; 1: unknown } | undefined,
  event: T,
) {
  if (typeof listener === "function") listener(event);
  else if (listener) listener[0](listener[1] as never, event);
}

export function composeButton(
  element: View,
  props: {
    children?: View;
    onClick?: (event: globalThis.MouseEvent) => void;
    attrs?: Record<string, string>;
  },
) {
  if (!(element instanceof HTMLElement)) return element;
  for (const [name, value] of Object.entries(props.attrs ?? {})) element.setAttribute(name, value);
  if (props.children !== undefined) {
    // The children are owned by the parent component even when attached to a supplied trigger.
    insert(element, () => props.children, null);
  }
  if (props.onClick) {
    const listener = props.onClick;
    element.addEventListener("click", listener);
    onCleanup(() => element.removeEventListener("click", listener));
  }
  return element;
}

export function splitProps<T extends object, K extends keyof T>(
  props: T,
  keys: readonly K[],
): [Pick<T, K>, Omit<T, K>] {
  const local = {} as Pick<T, K>;
  for (const key of keys)
    Object.defineProperty(local, key, { enumerable: true, get: () => props[key] });
  return [local, omit(props, ...keys) as unknown as Omit<T, K>];
}

export function ariaState<T extends string | boolean | null | undefined>(
  value: T,
): T extends boolean ? "true" | "false" : T {
  return (typeof value === "boolean" ? String(value) : value) as T extends boolean
    ? "true" | "false"
    : T;
}
