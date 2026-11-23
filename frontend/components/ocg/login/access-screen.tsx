import { RotateCw } from "@lucide/solid";
import { createSignal, Show } from "solid-js";
import { ApiError } from "@/components/ocg/runtime/api/transport";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { type View } from "@/lib/ui-state";
import { LanguageSwitcher, useI18n } from "../i18n";
import { OcgLogo } from "../primitives/ocg-logo";
import { WorkspaceIllustration } from "../primitives/workspace-illustration";

export function AccessScreen(props: { title: string; description: string; children: View }) {
  const i18n = useI18n();
  return (
    <div class="flex min-h-svh flex-col">
      <header class="flex h-16 shrink-0 items-center justify-between gap-4 border-b border-border bg-card px-5 sm:px-10">
        <div class="flex items-center gap-3">
          <OcgLogo />
          <span class="border-l border-border pl-3 text-sm text-muted-foreground">
            {i18n.t("settings.nav.workspace")}
          </span>
        </div>
        <LanguageSwitcher variant="select" />
      </header>
      <main class="mx-auto grid w-full max-w-5xl flex-1 content-center px-4 py-8 sm:px-8">
        <div class="grid overflow-hidden rounded-2xl border border-border bg-card shadow-sm md:grid-cols-2">
          <div class="flex items-center justify-center border-b border-border bg-primary/5 md:border-r md:border-b-0">
            <WorkspaceIllustration class="h-32 w-full sm:h-48 md:h-auto" />
          </div>
          <div class="flex min-w-0 flex-col justify-center gap-7 px-6 py-8 sm:p-10 md:min-h-96">
            <div>
              <h1 class="text-3xl leading-tight font-semibold tracking-tight">{props.title}</h1>
              <p class="mt-3 text-sm leading-6 text-muted-foreground">{props.description}</p>
            </div>
            {props.children}
          </div>
        </div>
      </main>
    </div>
  );
}

export function SessionAccess(props: {
  phase: "token" | "out" | "unreachable";
  signIn: (token: string) => Promise<void>;
}) {
  const i18n = useI18n();
  if (props.phase === "token") return <TokenSignIn signIn={props.signIn} />;
  const out = () => props.phase === "out";
  return (
    <AccessScreen
      title={i18n.t(out() ? "session.signedOutTitle" : "session.offlineTitle")}
      description={i18n.t(out() ? "session.signedOutDescription" : "session.unreachable")}
    >
      <Button class="self-start" onClick={() => window.location.reload()}>
        <RotateCw aria-hidden="true" />
        {i18n.t(out() ? "session.reopen" : "session.retry")}
      </Button>
    </AccessScreen>
  );
}

function TokenSignIn(props: { signIn: (token: string) => Promise<void> }) {
  const i18n = useI18n();
  const [token, setToken] = createSignal("");
  const [submitting, setSubmitting] = createSignal(false);
  const [error, setError] = createSignal<"session.tokenRejected" | "session.unreachable" | null>(
    null,
  );

  async function submit(event: SubmitEvent) {
    event.preventDefault();
    const value = token().trim();
    if (submitting() || !value) return;
    setError(null);
    setSubmitting(true);
    setToken("");
    try {
      await props.signIn(value);
    } catch (cause) {
      setError(
        cause instanceof ApiError &&
          (cause.status === 401 || cause.status === 403 || cause.code === "credential_unavailable")
          ? "session.tokenRejected"
          : "session.unreachable",
      );
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <AccessScreen
      title={i18n.t("session.tokenTitle")}
      description={i18n.t("session.tokenDescription")}
    >
      <form
        class="flex flex-col items-start gap-3"
        aria-busy={submitting() ? "true" : "false"}
        onSubmit={submit}
      >
        <label for="operator-api-token" class="text-sm font-medium">
          {i18n.t("session.tokenLabel")}
        </label>
        <Input
          id="operator-api-token"
          type="password"
          autocomplete="current-password"
          value={token()}
          disabled={submitting()}
          required
          onInput={(event) => {
            setToken(event.currentTarget.value);
            setError(null);
          }}
        />
        <Show when={error()} keyed>
          {(key) => (
            <p role="alert" class="text-sm text-destructive">
              {i18n.t(key)}
            </p>
          )}
        </Show>
        <Button type="submit" disabled={submitting() || !token().trim()}>
          {i18n.t("session.tokenSubmit")}
        </Button>
      </form>
    </AccessScreen>
  );
}
