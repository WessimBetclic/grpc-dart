// Copyright (c) 2026, the gRPC project authors. Please see the AUTHORS file
// for details. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

@TestOn('vm')
library;

import 'dart:async';

import 'package:fake_async/fake_async.dart';
import 'package:grpc/src/client/call.dart';
import 'package:grpc/src/client/client_keepalive.dart';
import 'package:grpc/src/client/client_transport_connector.dart';
import 'package:grpc/src/client/connection.dart';
import 'package:grpc/src/client/http2_connection.dart';
import 'package:grpc/src/client/method.dart';
import 'package:grpc/src/client/options.dart';
import 'package:grpc/src/client/transport/http2_credentials.dart';
import 'package:http2/transport.dart';
import 'package:test/test.dart';

/// The connection replaces its transport when it is too old, after a GOAWAY
/// or at the peer's concurrent-stream limit, and leaves the old one open with
/// its streams running. What the old transport does must not act on the one
/// that replaced it.
void main() {
  const pingInterval = Duration(seconds: 1);
  const pingTimeout = Duration(seconds: 2);
  const idleTimeout = Duration(seconds: 5);

  /// Long enough for a ping to go unanswered: one interval, then the timeout.
  const deadline = Duration(seconds: 4);

  late _Connector connector;
  late Http2ClientConnection connection;
  var pingTimeouts = 0;

  /// Runs [body] on a fake clock with a connection that pings every second
  /// and declares a transport dead two seconds after an unanswered ping.
  void run(
    String description,
    void Function(FakeAsync async) body, {
    Duration connectionTimeout = defaultConnectionTimeOut,
  }) {
    test(description, () {
      fakeAsync((async) {
        pingTimeouts = 0;
        connector = _Connector();
        connection = Http2ClientConnection.fromClientTransportConnector(
          connector,
          ChannelOptions(
            credentials: ChannelCredentials.insecure(),
            idleTimeout: idleTimeout,
            connectionTimeout: connectionTimeout,
            keepAlive: ClientKeepAliveOptions(
              pingInterval: pingInterval,
              timeout: pingTimeout,
              permitWithoutCalls: true,
              onPingTimeout: () => pingTimeouts++,
            ),
          ),
        );
        body(async);
      });
    });
  }

  /// Dispatches a call that opens no stream: enough for the connection to
  /// connect, or to retire a transport it no longer trusts.
  _Transport dispatch(FakeAsync async) {
    connection.dispatchCall(_cancelledCall());
    async.elapse(const Duration(milliseconds: 50));
    return connector.transports.last;
  }

  /// The server sent GOAWAY or reached its stream limit: [transport] can open
  /// no stream, and the next call replaces it.
  _Transport replace(FakeAsync async, _Transport transport) {
    transport.isOpen = false;
    return dispatch(async);
  }

  /// Elapses [duration] while each of [busy] receives a frame every 200 ms,
  /// as a transport carrying live data would.
  void elapse(
    FakeAsync async,
    Duration duration, {
    List<_Transport> busy = const [],
  }) {
    const step = Duration(milliseconds: 200);
    for (var elapsed = Duration.zero; elapsed < duration; elapsed += step) {
      async.elapse(step);
      for (final transport in busy) {
        transport.receive();
      }
    }
  }

  run('a replaced transport receiving data does not keep the current one '
      'from being pinged', (async) {
    final old = dispatch(async)..becomeActive();
    final current = replace(async, old)..becomeActive();

    elapse(async, deadline, busy: [old]);

    expect(current.pings, greaterThan(0));
    expect(current.terminations, 1, reason: 'it answered no ping');
    expect(pingTimeouts, 1);
  });

  run('a replaced transport going idle does not start the idle timer of the '
      'current one', (async) {
    final old = dispatch(async)..becomeActive();
    final current = replace(async, old)..becomeActive();

    old.becomeIdle();
    elapse(async, idleTimeout * 2, busy: [old, current]);

    expect(current.finishes, 0);
    expect(connection.state, ConnectionState.ready);
  });

  run('a replaced transport closing its socket does not abandon the current '
      'one', (async) {
    final old = dispatch(async)..becomeActive();
    final current = replace(async, old)..becomeActive();

    old.closeSocket();
    async.flushMicrotasks();

    expect(connection.state, ConnectionState.ready);
    expect(dispatch(async), same(current), reason: 'no third transport');

    elapse(async, deadline);
    expect(current.terminations, 1, reason: 'its keepalive still runs');
  });

  run('a replaced transport keeps its own keepalive until its socket closes', (
    async,
  ) {
    final old = dispatch(async)..becomeActive();
    final current = replace(async, old)..becomeActive();

    // Draining while answering: left to drain.
    elapse(async, deadline, busy: [old, current]);
    expect(old.terminations, 0);

    // Silent with a stream still bound: dead like any other transport, so the
    // stream fails instead of hanging.
    elapse(async, deadline, busy: [current]);
    expect(old.pings, greaterThan(0));
    expect(old.terminations, 1);
    expect(current.terminations, 0);

    // Its socket is gone: no longer pinged.
    final pings = old.pings;
    elapse(async, deadline, busy: [current]);
    expect(old.pings, pings);
  });

  run('a transport past its lifetime is finished and keeps its keepalive '
      'through the drain', (async) {
    final old = dispatch(async)..becomeActive();
    final current = dispatch(async)..becomeActive();
    expect(current, isNot(same(old)));
    expect(old.finishes, 1);

    elapse(async, deadline, busy: [current]);
    expect(old.terminations, 1, reason: 'it went silent while draining');
  }, connectionTimeout: Duration.zero);

  run('a transport abandoned at the peer\'s stream limit is closed once its '
      'streams end', (async) {
    final old = dispatch(async)..becomeActive();
    replace(async, old).becomeActive();
    expect(old.finishes, 0, reason: 'a GOAWAY now would race its streams');

    old.becomeIdle();
    expect(old.finishes, 1);
  });

  run('the current transport closing its socket still abandons the '
      'connection', (async) {
    final current = dispatch(async)..becomeActive();

    current.closeSocket();
    async.flushMicrotasks();

    expect(connection.state, ConnectionState.idle);
    expect(dispatch(async), isNot(same(current)));
  });
}

/// A call cancelled before it is dispatched: the connection connects for it,
/// or retires a transport it no longer trusts, and opens no stream.
ClientCall<void, void> _cancelledCall() => ClientCall(
  ClientMethod<void, void>('/noop', (_) => const [], (_) {}),
  const Stream<void>.empty(),
  CallOptions(),
)..isCancelled = true;

/// A transport driven by hand: what the server does to it (data, GOAWAY,
/// closing the socket) is simulated, what the connection asks of it (pings,
/// finish, terminate) is counted.
class _Transport implements ClientTransportConnection {
  final _frames = StreamController<void>.broadcast(sync: true);
  final _socketClosed = Completer<void>();
  ActiveStateHandler? _onActiveStateChanged;

  @override
  bool isOpen = true;

  int pings = 0;
  int finishes = 0;
  int terminations = 0;

  /// Completes when the socket under this transport is gone, as
  /// [ClientTransportConnector.done] does.
  Future<void> get socketClosed => _socketClosed.future;

  /// A frame from the server.
  void receive() => _frames.add(null);

  /// The first stream opens.
  void becomeActive() => _onActiveStateChanged?.call(true);

  /// The last stream ends.
  void becomeIdle() => _onActiveStateChanged?.call(false);

  /// The server closes the socket, or the drain started by [finish] ends.
  void closeSocket() {
    isOpen = false;
    if (!_socketClosed.isCompleted) _socketClosed.complete();
  }

  @override
  Stream<void> get onFrameReceived => _frames.stream;

  @override
  set onActiveStateChanged(ActiveStateHandler callback) =>
      _onActiveStateChanged = callback;

  @override
  Future<void> get onInitialPeerSettingsReceived => Future.value();

  @override
  Stream<int> get onPingReceived => const Stream.empty();

  @override
  Future<void> ping() async => pings++;

  @override
  Future<void> finish() async {
    finishes++;
    isOpen = false;
  }

  @override
  Future<void> terminate([int? errorCode]) async {
    terminations++;
    closeSocket();
  }

  @override
  ClientTransportStream makeRequest(
    List<Header> headers, {
    bool endStream = false,
  }) => throw UnsupportedError('no stream is opened in these tests');
}

class _Connector extends ClientTransportConnector {
  final transports = <_Transport>[];

  @override
  Future<ClientTransportConnection> connect() async {
    final transport = _Transport();
    transports.add(transport);
    return transport;
  }

  @override
  Future<void> get done => transports.last.socketClosed;

  @override
  String get authority => 'test';

  @override
  void shutdown() => transports.last.closeSocket();
}
