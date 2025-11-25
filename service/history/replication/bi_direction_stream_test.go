package replication

import (
	"context"
	"io"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.uber.org/mock/gomock"
)

type (
	biDirectionStreamTestDeps struct {
		controller           *gomock.Controller
		biDirectionStream    *BiDirectionStreamImpl[int, int]
		streamClientProvider *mockStreamClientProvider
		streamClient         *mockStreamClient
		streamErrClient      *mockStreamErrClient
	}

	mockStreamClientProvider struct {
		streamClient BiDirectionStreamClient[int, int]
	}
	mockStreamClient struct {
		shutdownChan chan struct{}

		requests []int

		responseCount int
		responses     []int
	}
	mockStreamErrClient struct {
		sendErr error
		recvErr error
	}
)

func setupBiDirectionStreamTest(t *testing.T) *biDirectionStreamTestDeps {
	controller := gomock.NewController(t)

	streamClient := &mockStreamClient{
		shutdownChan:  make(chan struct{}),
		requests:      nil,
		responseCount: 10,
		responses:     nil,
	}
	streamErrClient := &mockStreamErrClient{
		sendErr: serviceerror.NewUnavailable("random send error"),
		recvErr: serviceerror.NewUnavailable("random recv error"),
	}
	streamClientProvider := &mockStreamClientProvider{streamClient: streamClient}
	biDirectionStream := NewBiDirectionStream[int, int](
		streamClientProvider,
		metrics.NoopMetricsHandler,
		log.NewTestLogger(),
	)

	return &biDirectionStreamTestDeps{
		controller:           controller,
		biDirectionStream:    biDirectionStream,
		streamClientProvider: streamClientProvider,
		streamClient:         streamClient,
		streamErrClient:      streamErrClient,
	}
}

func TestLazyInit(t *testing.T) {
	deps := setupBiDirectionStreamTest(t)
	defer deps.controller.Finish()

	require.Nil(t, deps.biDirectionStream.streamingClient)

	deps.biDirectionStream.Lock()
	err := deps.biDirectionStream.lazyInitLocked()
	deps.biDirectionStream.Unlock()
	require.NoError(t, err)
	require.Equal(t, deps.streamClient, deps.biDirectionStream.streamingClient)
	require.True(t, deps.biDirectionStream.IsValid())

	deps.biDirectionStream.Lock()
	err = deps.biDirectionStream.lazyInitLocked()
	deps.biDirectionStream.Unlock()
	require.NoError(t, err)
	require.Equal(t, deps.streamClient, deps.biDirectionStream.streamingClient)
	require.True(t, deps.biDirectionStream.IsValid())

	deps.biDirectionStream.Close()
	deps.biDirectionStream.Lock()
	err = deps.biDirectionStream.lazyInitLocked()
	deps.biDirectionStream.Unlock()
	require.Error(t, err)
	require.False(t, deps.biDirectionStream.IsValid())
}

func TestSend(t *testing.T) {
	deps := setupBiDirectionStreamTest(t)
	defer deps.controller.Finish()
	defer close(deps.streamClient.shutdownChan)

	reqs := []int{rand.Int(), rand.Int(), rand.Int(), rand.Int()}
	for _, req := range reqs {
		err := deps.biDirectionStream.Send(req)
		require.NoError(t, err)
	}
	require.Equal(t, reqs, deps.streamClient.requests)
	require.True(t, deps.biDirectionStream.IsValid())
}

func TestSend_Err(t *testing.T) {
	deps := setupBiDirectionStreamTest(t)
	defer deps.controller.Finish()
	defer close(deps.streamClient.shutdownChan)

	deps.streamClientProvider.streamClient = deps.streamErrClient

	err := deps.biDirectionStream.Send(rand.Int())
	require.Error(t, err)
	require.False(t, deps.biDirectionStream.IsValid())
}

func TestRecv(t *testing.T) {
	deps := setupBiDirectionStreamTest(t)
	defer deps.controller.Finish()
	close(deps.streamClient.shutdownChan)

	var resps []int
	streamRespChan, err := deps.biDirectionStream.Recv()
	require.NoError(t, err)
	for streamResp := range streamRespChan {
		require.NoError(t, streamResp.Err)
		resps = append(resps, streamResp.Resp)
	}
	require.Equal(t, deps.streamClient.responses, resps)
	require.False(t, deps.biDirectionStream.IsValid())
}

func TestRecv_Err(t *testing.T) {
	deps := setupBiDirectionStreamTest(t)
	defer deps.controller.Finish()
	close(deps.streamClient.shutdownChan)
	deps.streamClientProvider.streamClient = deps.streamErrClient

	streamRespChan, err := deps.biDirectionStream.Recv()
	require.NoError(t, err)
	streamResp := <-streamRespChan
	require.Error(t, streamResp.Err)
	_, ok := <-streamRespChan
	require.False(t, ok)
	require.False(t, deps.biDirectionStream.IsValid())
}

func (p *mockStreamClientProvider) Get(
	_ context.Context,
) (BiDirectionStreamClient[int, int], error) {
	return p.streamClient, nil
}

func (c *mockStreamClient) Send(req int) error {
	c.requests = append(c.requests, req)
	return nil
}

func (c *mockStreamClient) Recv() (int, error) {
	if len(c.responses) >= c.responseCount {
		<-c.shutdownChan
		return 0, io.EOF
	}

	resp := rand.Int()
	c.responses = append(c.responses, resp)
	return resp, nil
}

func (c *mockStreamClient) CloseSend() error {
	return nil
}

func (c *mockStreamErrClient) Send(_ int) error {
	return c.sendErr
}

func (c *mockStreamErrClient) Recv() (int, error) {
	return 0, c.recvErr
}

func (c *mockStreamErrClient) CloseSend() error {
	return nil
}
