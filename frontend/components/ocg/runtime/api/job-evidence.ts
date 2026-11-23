import {
  ContractError,
  type JobToolsPage,
  type Output,
  type ProviderResponse,
  type ToolCall,
} from "@/components/ocg/contracts";
import type * as Api from "@/components/ocg/contracts/api";
import { type Send } from "./transport";
import { jobIdentity, sequence } from "./validators";

export function createJobEvidenceApi(send: Send) {
  const readJobToolsInternal = async (
    job: string,
    settings: { after?: string; limit?: number; signal?: AbortSignal } = {},
  ): Promise<JobToolsPage> => {
    jobIdentity(job);
    const query: string[] = [];
    if (settings.after !== undefined && settings.after !== null) {
      if (!/^[1-9][0-9]*:[0-9]+$/.test(settings.after))
        throw new ContractError("after", "a tool cursor <request>:<index>");
      query.push(`after=${settings.after}`);
    }
    if (settings.limit !== undefined) {
      if (!Number.isSafeInteger(settings.limit) || settings.limit < 1 || settings.limit > 100) {
        throw new ContractError("limit", "an integer from 1 to 100");
      }
      query.push(`limit=${settings.limit}`);
    }
    const suffix = query.length ? `?${query.join("&")}` : "";
    const answer = await send("GET", `/api/v1/jobs/${job}/tools${suffix}`, {
      signal: settings.signal,
    });
    return answer.input as Api.Tools;
  };

  return {
    async readOutputs(
      job: string,
      settings: {
        afterSeq?: number;
        limit?: number;
        signal?: AbortSignal;
      } = {},
    ) {
      jobIdentity(job);
      const query: string[] = [];
      if (settings.afterSeq !== undefined) query.push(`after_seq=${sequence(settings.afterSeq)}`);
      if (settings.limit !== undefined) {
        if (!Number.isSafeInteger(settings.limit) || settings.limit < 1 || settings.limit > 100) {
          throw new ContractError("limit", "an integer from 1 to 100");
        }
        query.push(`limit=${settings.limit}`);
      }
      const suffix = query.length ? `?${query.join("&")}` : "";
      const answer = await send("GET", `/api/v1/jobs/${job}/outputs${suffix}`, {
        signal: settings.signal,
      });
      return answer.input as Api.Outputs;
    },

    async readLatestOutput(job: string, signal?: AbortSignal): Promise<Output | null> {
      jobIdentity(job);
      const answer = await send("GET", `/api/v1/jobs/${job}/outputs/latest`, {
        signal,
      });
      const page = answer.input as Api.Outputs;
      return page.outputs[0] ?? null;
    },

    async readJobTool(
      job: string,
      request: number,
      index: number,
      signal?: AbortSignal,
    ): Promise<ToolCall> {
      jobIdentity(job);
      if (sequence(request) === 0 || request > 0xffffffff || sequence(index) > 0xffffffff)
        throw new ContractError("tool", "a positive request and unsigned index");
      const answer = await send("GET", `/api/v1/jobs/${job}/tools/${request}/${index}`, { signal });
      const page = answer.input as Api.Tools;
      const tool = page.tools[0];
      if (
        page.tools.length !== 1 ||
        !tool ||
        tool.request !== request ||
        tool.index !== index ||
        tool.arguments_cut ||
        tool.result_cut
      )
        throw new ContractError("tool", "the complete requested tool evidence");
      return tool;
    },

    async readToolResponseTime(job: string, request: number, signal?: AbortSignal) {
      jobIdentity(job);
      if (!Number.isSafeInteger(request) || request < 1)
        throw new ContractError("request", "a positive request number");
      const after = request === 1 ? "" : `&after=${request - 1}`;
      const page = (
        await send("GET", `/api/v1/jobs/${job}/responses?limit=1${after}`, {
          signal,
        })
      ).input as Api.ProviderResponses;
      const response = page.responses[0];
      return response?.request === request && response.requests_tools
        ? response.created_at_us
        : null;
    },

    async readAllProviderResponses(
      job: string,
      signal?: AbortSignal,
      after?: number,
    ): Promise<ProviderResponse[]> {
      jobIdentity(job);
      const responses: ProviderResponse[] = [];
      let cursor = after;
      let offset = 0;
      let partial: ProviderResponse | null = null;
      let partialBytes = 0;
      for (let pageCount = 0; pageCount < 50; pageCount++) {
        const query = `?limit=100${cursor === undefined ? "" : `&after=${sequence(cursor)}`}${offset ? `&text_offset=${offset}` : ""}`;
        const page = (await send("GET", `/api/v1/jobs/${job}/responses${query}`, { signal }))
          .input as Api.ProviderResponses;
        for (const chunk of page.responses) {
          if (chunk.text_offset === 0) {
            if (partial || chunk.request <= (cursor ?? 0))
              throw new ContractError("responses.request", "a request after the cursor");
            partial = {
              request: chunk.request,
              text: "",
              requests_tools: chunk.requests_tools,
              created_at_us: chunk.created_at_us,
            };
            partialBytes = 0;
          }
          if (
            !partial ||
            chunk.request !== partial.request ||
            chunk.text_offset !== partialBytes ||
            chunk.requests_tools !== partial.requests_tools ||
            chunk.created_at_us !== partial.created_at_us
          )
            throw new ContractError("responses.text_offset", "contiguous chunks of the same reply");
          partial.text += chunk.text;
          partialBytes += new TextEncoder().encode(chunk.text).length;
          if (chunk.text_complete) {
            responses.push(partial);
            partial = null;
          }
        }
        if (!page.has_more) return responses;
        if (
          page.next_after === null ||
          page.next_text_offset === null ||
          page.next_after < (cursor ?? 0) ||
          (page.next_after === cursor && page.next_text_offset <= offset)
        )
          throw new ContractError("next_after", "an advancing reply cursor");
        cursor = page.next_after;
        offset = page.next_text_offset;
      }
      throw new ContractError("responses", "complete provider replies within 50 pages");
    },

    async readAllJobTools(job: string, signal?: AbortSignal): Promise<ToolCall[]> {
      const tools: ToolCall[] = [];
      let cursor: string | undefined = undefined;
      for (let pageCount = 0; pageCount < 50; pageCount++) {
        const page = await readJobToolsInternal(job, {
          after: cursor,
          limit: 100,
          signal,
        });
        tools.push(...page.tools);
        if (!page.has_more || !page.next_after) {
          break;
        }
        cursor = page.next_after;
      }
      return tools;
    },
  };
}
