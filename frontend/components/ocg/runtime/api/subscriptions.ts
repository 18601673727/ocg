import { type Children, ContractError, type Projects } from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { type createProjectsApi } from "./projects-api";
import { type createStreams } from "./streams";
import { ApiError, type Transport } from "./transport";
import { jobIdentity } from "./validators";

export function createSubscriptions(
  transport: Transport,
  streams: ReturnType<typeof createStreams>,
  readProjectPage: ReturnType<typeof createProjectsApi>["readProjects"],
) {
  const { send, programmatic } = transport;
  const { browserLocks, subscribeSnapshots } = streams;
  let closed = false;
  type Conversations = Api.Conversations;
  type Observer<T> = {
    receive: (value: T) => void;
    error: (error: unknown) => void;
  };
  const catalogObservers = new Set<Observer<void>>();
  let stopCatalog: (() => void) | undefined;
  let catalogReady = false;
  let catalogError: unknown;
  const sidebarObservers = new Set<Observer<Conversations>>();
  const projectObservers = new Set<Observer<Projects>>();
  const childObservers = new Map<string, Map<Observer<Children>, number>>();
  const latestChildren = new Map<string, Children>();
  let stopSidebar: (() => void) | undefined;
  let sidebarQuery: string | undefined;
  let restartPending = false;
  let latestProjects: Projects | undefined;
  let latestConversations: Conversations | undefined;

  function subscribeCatalogInternal(receive: () => void, error: (error: unknown) => void) {
    const observer = { receive, error };
    catalogObservers.add(observer);
    if (!stopCatalog && !closed) {
      stopCatalog = subscribeSnapshots(
        "/api/v1/catalog/events",
        () => {
          catalogReady = true;
          catalogError = undefined;
          catalogObservers.forEach((item) => item.receive());
        },
        (cause) => {
          catalogReady = false;
          catalogError = cause;
          catalogObservers.forEach((item) => item.error(cause));
        },
      );
    } else if (catalogReady || catalogError !== undefined) {
      queueMicrotask(() => {
        if (!catalogObservers.has(observer)) return;
        if (catalogReady) observer.receive();
        else if (catalogError !== undefined) observer.error(catalogError);
      });
    }
    return () => {
      catalogObservers.delete(observer);
      if (!catalogObservers.size) {
        stopCatalog?.();
        stopCatalog = undefined;
        catalogReady = false;
        catalogError = undefined;
      }
    };
  }

  function finiteSidebar() {
    const controller = new AbortController();
    const signal = controller.signal;
    let dirty = true;
    let running = false;
    let retryDelay = 750;
    let timer: ReturnType<typeof setTimeout> | undefined;
    const report = (error: unknown) => {
      sidebarObservers.forEach((observer) => observer.error(error));
      projectObservers.forEach((observer) => observer.error(error));
      childObservers.forEach((observers) =>
        observers.forEach((_, observer) => observer.error(error)),
      );
    };
    const advance = (before: string | undefined, next: string | null | undefined) => {
      if (!next || (before !== undefined && next >= before))
        throw new ContractError("next_before", "an advancing sidebar cursor");
      return next;
    };
    const schedule = (delay = 750) => {
      if (signal.aborted || running || timer !== undefined) return;
      timer = setTimeout(() => {
        timer = undefined;
        void refresh();
      }, delay);
    };
    const refresh = async () => {
      if (signal.aborted || running || !dirty) return;
      running = true;
      dirty = false;
      let delay = 750;
      try {
        let before: string | undefined;
        let projects: Projects["projects"] = [];
        for (;;) {
          const page = await readProjectPage({ before, limit: 100, signal });
          projects = [...projects, ...page.projects];
          if (!page.has_more) {
            if (signal.aborted) return;
            latestProjects = { ...page, projects };
            projectObservers.forEach((observer) => observer.receive(latestProjects!));
            break;
          }
          before = advance(before, page.next_before);
        }
        before = undefined;
        let conversations: Conversations["conversations"] = [];
        for (;;) {
          const query = `?limit=100${before ? `&before=${jobIdentity(before)}` : ""}`;
          const page = (await send("GET", `/api/v1/conversations${query}`, { signal }))
            .input as Conversations;
          conversations = [...conversations, ...page.conversations];
          if (!page.has_more) {
            if (signal.aborted) return;
            latestConversations = { ...page, conversations };
            sidebarObservers.forEach((observer) => observer.receive(latestConversations!));
            break;
          }
          before = advance(before, page.next_before);
        }
        for (const [id, observers] of childObservers) {
          const maxPages = Math.max(...observers.values());
          let visiblePages = 0;
          let childBefore: string | undefined;
          let children: Children["children"] = [];
          for (;;) {
            const query = `?limit=20${childBefore ? `&before=${jobIdentity(childBefore)}` : ""}`;
            const page = (
              await send("GET", `/api/v1/conversations/${jobIdentity(id)}/children${query}`, {
                signal,
              })
            ).input as Children;
            children = [...children, ...page.children];
            if (page.children.some((child) => child.origin !== "system")) visiblePages++;
            if (!page.has_more || visiblePages >= maxPages) {
              if (signal.aborted) return;
              publishChildren(id, { ...page, children });
              break;
            }
            childBefore = advance(childBefore, page.next_before);
          }
        }
        retryDelay = 750;
      } catch (error) {
        if (!signal.aborted) {
          dirty = true;
          delay = retryDelay;
          retryDelay = Math.min(retryDelay * 2, 10000);
          report(error);
        }
      } finally {
        running = false;
        if (dirty) schedule(delay);
      }
    };
    const unsubscribe = subscribeCatalogInternal(() => {
      dirty = true;
      schedule();
    }, report);
    schedule(0);
    return () => {
      controller.abort();
      if (timer !== undefined) clearTimeout(timer);
      unsubscribe();
    };
  }

  function publishChildren(id: string, page: Children) {
    const observers = childObservers.get(id);
    if (!observers) return;
    latestChildren.set(id, page);
    observers.forEach((_, observer) => observer.receive(page));
  }

  function restartSidebar(force = false) {
    if (force) sidebarQuery = undefined;
    if (restartPending) return;
    restartPending = true;
    queueMicrotask(() => {
      restartPending = false;
      const watching =
        !closed && (sidebarObservers.size || projectObservers.size || childObservers.size);
      const query = watching
        ? [...childObservers]
            .map(([id, observers]) => `child=${jobIdentity(id)}:${Math.max(...observers.values())}`)
            .sort()
            .join("&")
        : undefined;
      if (query !== undefined && query === sidebarQuery) return;
      sidebarQuery = query;
      stopSidebar?.();
      stopSidebar = undefined;
      for (const id of latestChildren.keys()) {
        if (!childObservers.has(id)) latestChildren.delete(id);
      }
      if (query === undefined) return;
      if (!programmatic && !browserLocks()) {
        stopSidebar = finiteSidebar();
        return;
      }
      let conversations: Conversations["conversations"] = [];
      let projects: Projects["projects"] = [];
      const children = new Map<string, Children["children"]>();
      stopSidebar = subscribeSnapshots(
        `/api/v1/sidebar/events${query ? `?${query}` : ""}`,
        (snapshot) => {
          if (snapshot.topic === "conversations") {
            const page = snapshot.value as Api.Conversations;
            conversations = snapshot.first
              ? page.conversations
              : [...conversations, ...page.conversations];
            if (snapshot.last) {
              latestConversations = { ...page, conversations };
              sidebarObservers.forEach((observer) => observer.receive(latestConversations!));
            }
          } else if (snapshot.topic === "projects") {
            const page = snapshot.value as Projects;
            projects = snapshot.first ? page.projects : [...projects, ...page.projects];
            if (snapshot.last) {
              latestProjects = { ...page, projects };
              projectObservers.forEach((observer) => observer.receive(latestProjects!));
            }
          } else if (snapshot.topic === "children" && snapshot.key) {
            const page = snapshot.value as Children;
            const items = snapshot.first
              ? page.children
              : [...(children.get(snapshot.key) ?? []), ...page.children];
            children.set(snapshot.key, items);
            if (snapshot.last) publishChildren(snapshot.key, { ...page, children: items });
          } else throw new ContractError("snapshot.topic", "a sidebar topic");
        },
        (error) => {
          if (error instanceof ContractError || (error instanceof ApiError && !error.retryable)) {
            sidebarQuery = undefined;
          }
          sidebarObservers.forEach((observer) => observer.error(error));
          projectObservers.forEach((observer) => observer.error(error));
          childObservers.forEach((observers) =>
            observers.forEach((_, observer) => observer.error(error)),
          );
        },
        [
          "projects:",
          "conversations:",
          ...[...childObservers.keys()].map((id) => `children:${id}`),
        ],
      );
    });
  }

  return {
    restartSidebar,
    methods: {
      subscribeConversations(
        receive: Observer<Conversations>["receive"],
        error: Observer<Conversations>["error"],
      ) {
        const observer = { receive, error };
        sidebarObservers.add(observer);
        if (latestConversations) receive(latestConversations);
        restartSidebar();
        return () => {
          sidebarObservers.delete(observer);
          restartSidebar();
        };
      },
      subscribeProjects(
        receive: Observer<Projects>["receive"],
        error: Observer<Projects>["error"],
      ) {
        const observer = { receive, error };
        projectObservers.add(observer);
        if (latestProjects) receive(latestProjects);
        restartSidebar();
        return () => {
          projectObservers.delete(observer);
          restartSidebar();
        };
      },
      subscribeChildren(
        conversation: string,
        pages: number,
        receive: Observer<Children>["receive"],
        error: Observer<Children>["error"],
      ) {
        jobIdentity(conversation);
        const observer = { receive, error };
        const observers = childObservers.get(conversation) ?? new Map<Observer<Children>, number>();
        observers.set(observer, Math.min(Math.max(pages, 1), 100));
        childObservers.set(conversation, observers);
        const cached = latestChildren.get(conversation);
        if (cached) receive(cached);
        restartSidebar();
        return () => {
          observers.delete(observer);
          if (!observers.size) childObservers.delete(conversation);
          restartSidebar();
        };
      },
      subscribeLogs(
        file: string | undefined,
        receive: (value: Api.Logs) => void,
        error: (error: unknown) => void,
      ) {
        const query = file ? `?file=${encodeURIComponent(file)}` : "";
        return subscribeSnapshots(
          `/api/v1/logs/events${query}`,
          (snapshot) => {
            receive(snapshot.value as Api.Logs);
          },
          error,
        );
      },
      subscribeCatalog(receive: () => void, error: (error: unknown) => void) {
        return subscribeCatalogInternal(receive, error);
      },
      subscribeObservationChanges(
        receive: (value: {
          ready: boolean;
          usage: Api.Usage["observations"];
          availability: Api.Availability["observations"];
        }) => void,
        reset: () => void,
        error: (error: unknown) => void,
      ) {
        let cursor: string | undefined;
        let stopped = false;
        let stop: (() => void) | undefined;
        const start = () => {
          if (stopped) return;
          stop = subscribeSnapshots(
            () =>
              `/api/v1/observation-changes/events${cursor ? `?since=${encodeURIComponent(cursor)}` : ""}`,
            (snapshot) => {
              const value = snapshot.value as {
                usage: Api.Usage["observations"];
                availability: Api.Availability["observations"];
              };
              const { usage, availability } = value;
              receive({ ready: snapshot.first, usage, availability });
              cursor = snapshot.key ?? undefined;
            },
            (cause) => {
              if (cause instanceof ApiError && cause.code === "observation_replay_lost") {
                cursor = undefined;
                reset();
                queueMicrotask(start);
              } else error(cause);
            },
          );
        };
        start();
        return () => {
          stopped = true;
          stop?.();
        };
      },
    },
    close() {
      closed = true;
      stopSidebar?.();
      sidebarObservers.clear();
      projectObservers.clear();
      childObservers.clear();
    },
  };
}
