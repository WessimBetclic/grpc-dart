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
import 'dart:io';

import 'package:grpc/grpc.dart';
import 'package:http2/transport.dart';
import 'package:test/test.dart';

import '../src/client_utils.dart';

/// Stands in for the socket of a connection the OS closed behind the client's
/// back (a phone coming back from the background): the first HEADERS frame
/// written to it fails the way `_NativeSocket.write` fails, with the error
/// reported through the socket's stream one microtask later.
class _ClosedSocket implements StreamSink<List<int>> {
  _ClosedSocket(this._fromServer);

  final StreamController<List<int>> _fromServer;
  final _done = Completer<void>();
  bool _failed = false;

  @override
  void add(List<int> bytes) {
    if (_failed || !_carriesHeadersFrame(bytes)) return;
    _failed = true;
    scheduleMicrotask(
      () => _fromServer.addError(const SocketException('Write failed')),
    );
  }

  @override
  void addError(Object error, [StackTrace? stackTrace]) {}

  @override
  Future<void> addStream(Stream<List<int>> stream) => stream.forEach(add);

  @override
  Future<void> close() {
    if (!_done.isCompleted) _done.complete();
    return _done.future;
  }

  @override
  Future<void> get done => _done.future;

  static const _headersFrameType = 0x1;
  static const _frameHeaderLength = 9;
  static const _connectionPrefaceStart = 0x50; // 'P' of "PRI * HTTP/2.0".

  static bool _carriesHeadersFrame(List<int> bytes) {
    if (bytes.isEmpty || bytes.first == _connectionPrefaceStart) return false;
    var offset = 0;
    while (offset + _frameHeaderLength <= bytes.length) {
      final length =
          (bytes[offset] << 16) | (bytes[offset + 1] << 8) | bytes[offset + 2];
      if (bytes[offset + 3] == _headersFrameType) return true;
      offset += _frameHeaderLength + length;
    }
    return false;
  }
}

void main() {
  test(
    'a request still in flight when the connection dies is dropped, not raised',
    () async {
      final fromServer = StreamController<List<int>>(sync: true);
      addTearDown(fromServer.close);
      final transport = ClientTransportConnection.viaStreams(
        fromServer.stream,
        _ClosedSocket(fromServer),
      );
      final options = FakeChannelOptions();
      final channel = FakeChannel(
        'test',
        FakeConnection('test', transport, options),
        options,
      );
      final client = TestClient(channel);

      final uncaught = <Object>[];
      Object? failure;
      await runZonedGuarded(() async {
        try {
          await client.serverStreaming(1).toList();
        } catch (error) {
          failure = error;
        }
        // The request reaches the HTTP/2 stream only after the call failed.
        await Future<void>.delayed(Duration.zero);
      }, (error, stack) => uncaught.add(error));

      expect(
        failure,
        isA<GrpcError>()
            .having((error) => error.code, 'code', StatusCode.unknown)
            .having(
              (error) => error.message,
              'message',
              contains('forcefully terminated'),
            ),
      );
      expect(uncaught, isEmpty);
    },
  );
}
