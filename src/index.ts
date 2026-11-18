import { Request, Response, RequestResponse, Notification } from './studio';

import { get_encoder, get_decoder } from './framing';
import { RpcTransport } from './transport';

import { Mutex } from 'async-mutex';
import { ErrorConditions } from './meta';
export { Request, RequestResponse, Response, Notification };

export type {
  HsbColor,
  RgbUnderglowState,
  SetRgbUnderglowStateRequest,
  BacklightState,
  SetBacklightStateRequest,
  Request as LightingRequest,
  Response as LightingResponse,
  Notification as LightingNotification,
} from './lighting';

export interface RpcConnection {
  label: string;
  request_response_readable: ReadableStream<RequestResponse>;
  request_writable: WritableStream<Request>;
  notification_readable: ReadableStream<Notification>;
  current_request: number;
}

export interface CreateRpcConnectionOpts {
  signal?: AbortSignal;
}

export function create_rpc_connection(transport: RpcTransport, opts?: CreateRpcConnectionOpts): RpcConnection {
  let { writable: request_writable, readable: byte_readable } =
    new TransformStream<Request, Uint8Array>({
      transform(chunk, controller) {
        let bytes = Request.encode(chunk).finish();
        controller.enqueue(bytes);
      },
    });

  let reqPipelineClosed = byte_readable
    .pipeThrough(new TransformStream(get_encoder()), { signal: opts?.signal })
    .pipeTo(transport.writable, { signal: opts?.signal });

  reqPipelineClosed.catch((r) => {console.log("Closed error", r); return r}).then(async (reason: any) => {
    await byte_readable.cancel();
    transport.abortController.abort(reason);
  });

  let response_readable = transport.readable
    .pipeThrough(new TransformStream(get_decoder()), { signal: opts?.signal })
    .pipeThrough(
      new TransformStream({
        transform(chunk, controller) {
          controller.enqueue(Response.decode(chunk));
        },
      }),
      { signal: opts?.signal }
    );

  let [a, b] = response_readable.tee();

  let request_response_readable = a.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        if (chunk.requestResponse) {
          controller.enqueue(chunk.requestResponse);
        }
      },
    }),
    { signal: opts?.signal }
  );

  let notification_readable = b.pipeThrough(
    new TransformStream({
      transform(chunk, controller) {
        if (chunk.notification) {
          controller.enqueue(chunk.notification);
        }
      },
    }),
    { signal: opts?.signal }
  );

  return {
    label: transport.label,
    request_response_readable,
    request_writable,
    notification_readable,
    current_request: 0,
  };
}

const rpcMutex = new Mutex();

async function writeRequest(conn: RpcConnection, request: Request): Promise<void> {
  const writer = conn.request_writable.getWriter();
  try {
    await writer.write(request);
  } finally {
    writer.releaseLock();
  }
}

export class NoResponseError extends Error {
  constructor() {
    super("No RPC response received");
    Object.setPrototypeOf(this, NoResponseError.prototype);
  }
}

export class MetaError extends Error {
  readonly condition: ErrorConditions;

  constructor(condition: ErrorConditions) {
    super("Meta error: " + condition);
    this.condition = condition;
    Object.setPrototypeOf(this, MetaError.prototype);
  }
}

export type RpcRequest = Omit<Request, "requestId" | "supportsActionConfirmation" | "confirmAction">;

export async function call_rpc(
  conn: RpcConnection,
  req: RpcRequest
): Promise<RequestResponse> {
  return await rpcMutex.runExclusive(async () => {
    const request: Request = {
      ...req,
      requestId: conn.current_request++,
      supportsActionConfirmation: true,
    };
    await writeRequest(conn, request);

    const reader = conn.request_response_readable.getReader();
    const { done, value } = await reader.read().finally(() => reader.releaseLock());

    if (done || !value) {
      throw 'No response';
    }

    if (value.requestId != request.requestId) {
      throw 'Mismatch request IDs';
    }

    if (value.meta?.noResponse) {
      throw new NoResponseError();
    } else if (value.meta?.simpleError) {
      throw new MetaError(value.meta.simpleError);
    }

    if (value.actionToken) {
      await writeRequest(conn, {
        requestId: conn.current_request++,
        supportsActionConfirmation: true,
        confirmAction: value.actionToken,
      });
    }
    return value;
  });
}
