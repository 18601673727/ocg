import { render } from "@solidjs/web";
import { createEffect, createSignal, onCleanup, Show } from "solid-js";
import { dictionaries } from "@/components/ocg/i18n/dictionaries";
import { DEFAULT_LOCALE, isLocale } from "@/components/ocg/i18n/locale";
import { type ApiClient, ApiError, createApiClient } from "@/components/ocg/runtime/api-client";
import type { OperatorConsoleProps } from "@/components/ocg/session/operator-console";
import "../styles/globals.css";
import { ThemeProvider } from "@/components/ocg/appearance/theme-provider";
import { I18nProvider } from "@/components/ocg/i18n";
import { OperatorConsole } from "@/components/ocg/session/operator-console";

type OperatorSession = Awaited<ReturnType<ApiClient["readSession"]>>;

function unauthorized(error: unknown) {
  return error instanceof ApiError && error.status === 401;
}

function SessionLoading() {
  const language = document.documentElement.lang;
  const locale = isLocale(language) ? language : DEFAULT_LOCALE;
  return (
    <main class="flex flex-1 items-center justify-center">
      <p role="status" class="flex items-center gap-2 text-sm text-muted-foreground">
        <span
          aria-hidden="true"
          class="size-4 rounded-full border-2 border-primary border-t-transparent motion-safe:animate-spin"
        />
        {dictionaries[locale]["session.checking"]}
      </p>
    </main>
  );
}

function App() {
  const [phase, setPhase] = createSignal<OperatorConsoleProps["phase"] | null>(null);
  const [session, setSession] = createSignal<OperatorSession | null>(null);
  const [health, setHealth] = createSignal<OperatorConsoleProps["health"]>(null);
  const [signingOut, setSigningOut] = createSignal(false);
  const [signOutErrorCode, setSignOutErrorCode] = createSignal<string | null>(null);
  let csrf = "";
  const client = createApiClient({
    baseUrl: window.location.origin,
    fetch: window.fetch.bind(window),
    csrfToken: () => csrf,
  });

  const initial = new AbortController();
  void client
    .readSession(initial.signal)
    .then((signed) => {
      if (initial.signal.aborted) return;
      csrf = signed.csrfToken;
      setSession(signed);
      setPhase("in");
    })
    .catch(() => {
      if (!initial.signal.aborted) setPhase("unreachable");
    });
  onCleanup(() => {
    initial.abort();
    client.close();
  });

  createEffect(
    () => phase() === "in" && !signingOut(),
    (active) => {
      if (!active) return;
      const controller = new AbortController();
      const { signal } = controller;
      let reading = false;

      const disconnected = () => {
        csrf = "";
        setSession(null);
        setPhase("unreachable");
      };

      const read = async () => {
        if (reading) return;
        reading = true;
        try {
          const answer = await client.readHealth(signal);
          if (!signal.aborted) setHealth(answer);
        } catch (error: unknown) {
          if (signal.aborted) return;
          setHealth(null);
          if (!unauthorized(error)) return;
          try {
            const renewed = await client.readSession(signal);
            if (signal.aborted) return;
            csrf = renewed.csrfToken;
            setSession(renewed);
          } catch {
            if (!signal.aborted) disconnected();
            return;
          }
          try {
            const answer = await client.readHealth(signal);
            if (!signal.aborted) setHealth(answer);
          } catch (retry: unknown) {
            if (!signal.aborted && unauthorized(retry)) disconnected();
          }
        } finally {
          reading = false;
        }
      };

      const unsubscribe = client.subscribeCatalog(
        () => void read(),
        () => {
          if (!signal.aborted) setHealth(null);
        },
      );
      const timer = setInterval(() => void read(), 5000);
      void read();
      return () => {
        controller.abort();
        clearInterval(timer);
        unsubscribe();
      };
    },
  );

  async function signOut() {
    if (phase() !== "in" || signingOut()) return;
    setSigningOut(true);
    setSignOutErrorCode(null);
    try {
      try {
        await client.deleteSession(initial.signal);
      } catch (error: unknown) {
        if (!unauthorized(error)) throw error;
      }
      if (initial.signal.aborted) return;
      csrf = "";
      setSession(null);
      setHealth(null);
      setPhase("out");
    } catch (error: unknown) {
      if (initial.signal.aborted) return;
      setSignOutErrorCode(error instanceof ApiError ? error.code : "invalid_response");
    } finally {
      if (!initial.signal.aborted) setSigningOut(false);
    }
  }

  return (
    <Show when={phase()} fallback={<SessionLoading />}>
      {(currentPhase) => (
        <ThemeProvider>
          <I18nProvider>
            <OperatorConsole
              phase={currentPhase()}
              client={client}
              health={health()}
              sessionExpiresAtUs={session()?.expiresAtUs ?? null}
              signingOut={signingOut()}
              signOutErrorCode={signOutErrorCode()}
              signOut={signOut}
            />
          </I18nProvider>
        </ThemeProvider>
      )}
    </Show>
  );
}

const root = document.getElementById("root");
if (root) render(() => <App />, root);
